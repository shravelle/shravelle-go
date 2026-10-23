package shravelle

// Runtime metrics for the Runtime tab in APM: CPU, memory, heap, GC and
// goroutines, read from runtime/metrics (no extra dependency).
//
//	process.cpu.utilization   fraction of all CPUs (0..1)
//	process.memory.usage      resident memory, bytes (Linux; Go-managed memory elsewhere)
//	go.memory.used            Go-managed memory, bytes
//	go.memory.gc.goal         heap size at which the next GC starts, bytes
//	go.memory.allocated       bytes allocated since start (cumulative)
//	go.gc.count               GC cycles since start (cumulative)
//	go.goroutine.count        goroutines

import (
	"context"
	"os"
	"runtime"
	rm "runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

func registerRuntimeMetrics(m metric.Meter) error {
	names := []string{"/cpu/classes/total:cpu-seconds", "/cpu/classes/idle:cpu-seconds", "/memory/classes/total:bytes", "/memory/classes/heap/released:bytes",
		"/gc/heap/goal:bytes", "/gc/heap/allocs:bytes", "/gc/cycles/total:gc-cycles", "/sched/goroutines:goroutines"}
	samples := make([]rm.Sample, len(names))
	for i, n := range names {
		samples[i].Name = n
	}
	var mu sync.Mutex
	var lastCPU float64
	var lastAt time.Time
	val := func(s rm.Sample) float64 {
		switch s.Value.Kind() {
		case rm.KindUint64:
			return float64(s.Value.Uint64())
		case rm.KindFloat64:
			return s.Value.Float64()
		}
		return 0
	}
	cpuUtil, _ := m.Float64ObservableGauge("process.cpu.utilization", metric.WithUnit("1"), metric.WithDescription("CPU used by the process, fraction of all CPUs"))
	rss, _ := m.Int64ObservableGauge("process.memory.usage", metric.WithUnit("By"))
	used, _ := m.Int64ObservableGauge("go.memory.used", metric.WithUnit("By"))
	goal, _ := m.Int64ObservableGauge("go.memory.gc.goal", metric.WithUnit("By"))
	alloc, _ := m.Int64ObservableGauge("go.memory.allocated", metric.WithUnit("By"))
	gcs, _ := m.Int64ObservableGauge("go.gc.count", metric.WithUnit("{gc_cycle}"))
	gor, _ := m.Int64ObservableGauge("go.goroutine.count", metric.WithUnit("{goroutine}"))
	_, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		mu.Lock()
		defer mu.Unlock()
		rm.Read(samples)
		now := time.Now()
		// CPU seconds actually used: the process's user+system time on Linux,
		// else the Go runtime's estimate (available minus idle).
		cpu := linuxCPUSeconds()
		if cpu <= 0 {
			cpu = val(samples[0]) - val(samples[1])
		}
		if !lastAt.IsZero() {
			if wall := now.Sub(lastAt).Seconds(); wall > 0 {
				o.ObserveFloat64(cpuUtil, max(0, (cpu-lastCPU)/wall/float64(runtime.NumCPU())))
			}
		}
		lastCPU, lastAt = cpu, now
		goMem := int64(val(samples[2]) - val(samples[3]))
		o.ObserveInt64(used, goMem)
		if r := linuxRSS(); r > 0 {
			o.ObserveInt64(rss, r)
		} else {
			o.ObserveInt64(rss, goMem)
		}
		o.ObserveInt64(goal, int64(val(samples[4])))
		o.ObserveInt64(alloc, int64(val(samples[5])))
		o.ObserveInt64(gcs, int64(val(samples[6])))
		o.ObserveInt64(gor, int64(val(samples[7])))
		return nil
	}, cpuUtil, rss, used, goal, alloc, gcs, gor)
	return err
}

// linuxRSS reads the resident set size from /proc/self/statm (0 elsewhere).
func linuxRSS() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.ParseInt(f[1], 10, 64)
	return pages * int64(os.Getpagesize())
}

// linuxCPUSeconds returns user+system CPU seconds of this process from
// /proc/self/stat (0 elsewhere). Clock ticks are 100 Hz on Linux.
func linuxCPUSeconds() float64 {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')') // the command name may contain spaces
	if i < 0 {
		return 0
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 13 {
		return 0
	}
	ut, _ := strconv.ParseFloat(f[11], 64)
	st, _ := strconv.ParseFloat(f[12], 64)
	return (ut + st) / 100
}
