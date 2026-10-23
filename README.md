# Go

```bash
go get github.com/shravelle/shravelle-go   # module in sdks/shravelle-sdk-go; run `go mod tidy`
```
```go
stop, err := shravelle.Start(ctx, shravelle.Config{ServiceName: "checkout"}) // env: SHRAVELLE_API_KEY, SHRAVELLE_ENDPOINT
if err != nil { log.Fatal(err) }
defer stop(context.Background())

mux := http.NewServeMux()
mux.HandleFunc("GET /orders/{id}", getOrder)
http.ListenAndServe(":8080", shravelle.HTTPHandler(mux, "server")) // spans named "GET /orders/{id}"
client := shravelle.HTTPClient()                                   // outbound calls + traceparent
```
`Config`: `Endpoint`, `APIKey`, `ServiceName`, `ServiceVersion`, `Environment`, `SampleRate`,
`MetricInterval`, `Attributes`. Go has no runtime auto-instrumentation: wrap handlers with `HTTPHandler`,
clients with `HTTPClient`, and use OTel contrib packages for the rest (below).

## API
```go
ctx, end := shravelle.Segment(ctx, "pricing.calculate", attribute.String("sku", sku)); defer end()
shravelle.NoticeError(ctx, err, attribute.Int("order_id", id))
defer shravelle.Recover(ctx)            // records a panic as an error, then re-panics
shravelle.SetUser(ctx, userID)
shravelle.AddAttributes(ctx, attribute.Float64("cart_value", 1450))
shravelle.RecordEvent(ctx, "coupon_applied", attribute.String("code", "DIWALI"))
shravelle.Meter().Int64Counter("orders_total")
shravelle.RecordDeployment(ctx, "1.4.2", sha, "release notes")
```
Databases and other libraries: use the OTel contrib packages (`otelsql`, `otelgrpc`, `otelpgx`,
`redisotel`, `otelmongo`, `otelsarama`, …). They use the global providers that `Start` installs.
Logs: `log/slog` with the OTel bridge (`go.opentelemetry.io/contrib/bridges/otelslog`).


---
Part of [Shravelle](https://shravelle.com). Setup guide for every platform: [shravelle.com/docs](https://shravelle.com/docs.html) · Get your keys: [shravelle.com/get-started](https://shravelle.com/get-started.html)
