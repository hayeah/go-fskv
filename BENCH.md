270kb get/put with random keys

```
go test -bench=FileStore
BenchmarkFileStorePutRandom-4               2000           1059099 ns/op
BenchmarkFileStoreGetRandom-4              20000             71007 ns/op
```