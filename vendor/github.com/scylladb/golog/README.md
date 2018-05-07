# Golog

This is a wrapper over [Uber zap](https://github.com/uber-go/zap) library that replaces the sugared logger. 

Features:

* Syslog integration
* Automatic stacktraces for errors
* Context aware tracing ID
* Easy to use
* Fast

Example:

```go
logger, err := golog.NewProduction(golog.Config{
	Mode:  golog.SyslogMode,
	Level: zapcore.InfoLevel,
})
if err != nil {
	t.Fatal(err)
}
logger.Info(ctx, "Could not connect to database",
	"sleep", 5*time.Second,
	"error", errors.New("I/O error"),
)
logger.Named("sub").Error(ctx, "Unexpected error", "error", errors.New("unexpected"))
```

Produces:

```
Apr 25 14:32:37 localhost.localdomain /tmp/___TestExample_in_github_com_scylladb_golog[30340]: {"msg":"Could not connect to database","sleep":5,"error":"I/O error","_trace_id":"kQ8Z_01bS26Vlz74VFaYLA"}
Apr 25 14:32:37 localhost.localdomain /tmp/___TestExample_in_github_com_scylladb_golog[30340]: {"logger":"sub","msg":"Unexpected error","error":"unexpected","_trace_id":"kQ8Z_01bS26Vlz74VFaYLA","stacktrace":"github.com/scylladb/golog.Logger.log\n\t/home/michal/work/scylla/golog/src/github.com/scylladb/golog/logger.go:80\ngithub.com/scylladb/golog.Logger.Error\n\t/home/michal/work/scylla/golog/src/github.com/scylladb/golog/logger.go:63\ngithub.com/scylladb/golog_test.TestExample\n\t/home/michal/work/scylla/golog/src/github.com/scylladb/golog/example_test.go:28\ntesting.tRunner\n\t/home/michal/work/tools/go/go1.10.linux-amd64/src/testing/testing.go:777"}
```

## Benchmarks

Benchmark results of running against zap and zap sugared loggers on Intel(R) Core(TM) i7-7500U CPU @ 2.70GHz.

```
BenchmarkZap-4                   2000000               978 ns/op             256 B/op          1 allocs/op
BenchmarkZapSugared-4            1000000              1353 ns/op             528 B/op          2 allocs/op
BenchmarkLogger-4                1000000              1167 ns/op             256 B/op          1 allocs/op
```
