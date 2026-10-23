package shravelle

// Continuous profiling for Go services.
//
// Every ProfileInterval (default 60 s) the SDK uploads to the Shravelle ingest
// gateway (POST /v1/profiles):
//
//   - cpu          a ProfileCPUDuration (default 10 s) CPU profile (pprof)
//   - inuse_space  live heap bytes per allocation stack (snapshot)
//   - inuse_objects
//   - alloc_space  bytes allocated since the previous upload (delta)
//   - alloc_objects
//   - goroutines   goroutines per stack (pprof)
//
// Overhead: CPU profiling samples at 100 Hz for 10 s per minute (~1-2 %);
// heap data comes from the runtime's existing allocation sampling
// (runtime.MemProfileRate, default 512 KiB). Disable with
// SHRAVELLE_PROFILING=false or Config.DisableProfiling.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

type allocRec struct{ allocBytes, allocObjs int64 }

type profiler struct {
	c       Config
	hc      *http.Client
	host    string
	mu      sync.Mutex
	prev    map[[32]uintptr]allocRec
	names   map[[32]uintptr]string
	started time.Time
	cancel  context.CancelFunc
	done    chan struct{}
}

func startProfiler(c Config) func() {
	host, _ := os.Hostname()
	ctx, cancel := context.WithCancel(context.Background())
	p := &profiler{c: c, hc: &http.Client{Timeout: 30 * time.Second}, host: host,
		prev: map[[32]uintptr]allocRec{}, names: map[[32]uintptr]string{}, cancel: cancel, done: make(chan struct{})}
	p.heap(true) // baseline for alloc deltas
	go p.loop(ctx)
	return func() { cancel(); <-p.done }
}

func (p *profiler) loop(ctx context.Context) {
	defer close(p.done)
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		start := time.Now()
		p.cpu(ctx)
		p.heap(false)
		p.goroutines()
		t.Reset(max(time.Second, p.c.ProfileInterval-time.Since(start)))
	}
}

func (p *profiler) upload(params url.Values, contentType string, body []byte) {
	params.Set("service", p.c.ServiceName)
	params.Set("env", p.c.Environment)
	params.Set("version", p.c.ServiceVersion)
	params.Set("host", p.host)
	params.Set("language", "go")
	req, err := http.NewRequest("POST", p.c.Endpoint+"/v1/profiles?"+params.Encode(), bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-API-Key", p.c.APIKey)
	resp, err := p.hc.Do(req)
	if err != nil {
		return // best effort: the next interval sends fresh data
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != 429 && resp.StatusCode != 503 {
		log.Printf("shravelle: profile upload rejected: HTTP %d", resp.StatusCode)
	}
}

func (p *profiler) cpu(ctx context.Context) {
	var buf bytes.Buffer
	if err := pprof.StartCPUProfile(&buf); err != nil {
		return // another CPU profile (e.g. net/http/pprof) is running
	}
	select {
	case <-ctx.Done():
	case <-time.After(p.c.ProfileCPUDuration):
	}
	pprof.StopCPUProfile()
	p.upload(url.Values{}, "application/octet-stream", buf.Bytes())
}

func (p *profiler) goroutines() {
	var buf bytes.Buffer
	if pr := pprof.Lookup("goroutine"); pr != nil && pr.WriteTo(&buf, 0) == nil {
		p.upload(url.Values{}, "application/octet-stream", buf.Bytes())
	}
}

// scale undoes the runtime's allocation sampling (same formula as pprof).
func scale(count, size int64) (int64, int64) {
	rate := int64(runtime.MemProfileRate)
	if count == 0 || size == 0 {
		return 0, 0
	}
	if rate <= 1 {
		return count, size
	}
	avg := float64(size) / float64(count)
	s := 1 / (1 - math.Exp(-avg/float64(rate)))
	return int64(float64(count) * s), int64(float64(size) * s)
}

func (p *profiler) name(r *runtime.MemProfileRecord) string {
	if n, ok := p.names[r.Stack0]; ok {
		return n
	}
	var leafFirst []string
	frames := runtime.CallersFrames(r.Stack())
	for {
		f, more := frames.Next()
		if f.Function != "" {
			leafFirst = append(leafFirst, strings.NewReplacer(";", ":", " ", "_").Replace(f.Function))
		}
		if !more {
			break
		}
	}
	for i, j := 0, len(leafFirst)-1; i < j; i, j = i+1, j-1 {
		leafFirst[i], leafFirst[j] = leafFirst[j], leafFirst[i]
	}
	n := strings.Join(leafFirst, ";")
	p.names[r.Stack0] = n
	return n
}

// heap uploads live heap (snapshot) and allocations since the last call (delta).
func (p *profiler) heap(baseline bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, _ := runtime.MemProfile(nil, true)
	recs := make([]runtime.MemProfileRecord, n+64)
	n, ok := runtime.MemProfile(recs, true)
	if !ok {
		return
	}
	recs = recs[:n]
	var inuseB, inuseO, allocB, allocO strings.Builder
	cur := make(map[[32]uintptr]allocRec, len(recs))
	for i := range recs {
		r := &recs[i]
		cur[r.Stack0] = allocRec{r.AllocBytes, r.AllocObjects}
		if baseline {
			continue
		}
		stack := p.name(r)
		if stack == "" {
			continue
		}
		if objs, bytes := scale(r.InUseObjects(), r.InUseBytes()); bytes > 0 {
			fmt.Fprintf(&inuseB, "%s %d\n", stack, bytes)
			fmt.Fprintf(&inuseO, "%s %d\n", stack, objs)
		}
		prev := p.prev[r.Stack0]
		if objs, bytes := scale(r.AllocObjects-prev.allocObjs, r.AllocBytes-prev.allocBytes); bytes > 0 {
			fmt.Fprintf(&allocB, "%s %d\n", stack, bytes)
			fmt.Fprintf(&allocO, "%s %d\n", stack, objs)
		}
	}
	p.prev = cur
	if baseline {
		return
	}
	dur := fmt.Sprint(int(p.c.ProfileInterval.Seconds()))
	for typ, b := range map[string]*strings.Builder{"inuse_space": &inuseB, "inuse_objects": &inuseO, "alloc_space": &allocB, "alloc_objects": &allocO} {
		if b.Len() == 0 {
			continue
		}
		unit := "bytes"
		if strings.HasSuffix(typ, "objects") {
			unit = "count"
		}
		p.upload(url.Values{"type": {typ}, "unit": {unit}, "duration": {dur}}, "text/plain", []byte(b.String()))
	}
}
