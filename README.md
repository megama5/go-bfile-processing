# go-bfile-processing
An example of processing a big file with IPv4 addresses using Bloom Filter and butch processing


## NOTE!

With the current config, the program processes 120 GB file in 1HR, but with 3.1GB of RAM consumption.
Decreasing chunkSize, readBufferSize, and workersCount will decrease RAM consumption, but
increases execution time.
```aiignore
chunkSize = mb * 100
readBufferSize = 10
workersCount = 11
bfElementsCount = 1 << 16
```

Increasing the bit set size with bfElementsCount will give more accurate results.
```
bfElementsCount = 1 << 32 // IDEAL for IPv4
```

Output result is in `result.png`
