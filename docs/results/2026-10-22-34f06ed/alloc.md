client + server in one process, sequential; counts allocator calls, not memory in use

| cenário | allocs/call | bytes/call |
| --- | ---: | ---: |
| invoke system.noop | 17.00 | 1713.00 |
| invoke system.info | 34.00 | 3213.00 |
| ping | 7.00 | 142.00 |
| batch x20 system.noop (por batch) | 249.00 | 33768.00 |
| batch x20 system.noop (por chamada interna) | 12.45 | 1688.40 |
