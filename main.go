package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

const (
	fileName = "ip_addresses" // a file with IP addresses.
	mb       = 1000000        // bytes in 1 MB

	// Read chunk size. The less the size, the less memory the program will use.
	chunkSize = mb * 100
	// A chan size, that processes read chuck.
	readBufferSize = 10
	// Workers count
	workersCount = 11

	// Approximate unique element count for the Bloom Filtering.
	// NOTE! Ideally, we should use 1 << 32, but such config requires TONS of RAM.
	// Using 1<<16 will do the trick, but the result won't be as accurate as needed in real life.
	bfElementsCount = 1 << 16

	/**
	NOTE!

	With the current config, the program processes 120 GB file in 1HR, but with 3.1GB of RAM consumption.
	Decreasing chunkSize, readBufferSize, and workersCount will decrease RAM consumption, but
	increases execution time.

	Increase the bit set size with bfElementsCount will give more accurate results.
	*/
)

// CountUniqueIPs counts unique IPs in the file
func CountUniqueIPs(filename string) (uint64, error) {
	now := time.Now()
	println(now.String())
	runtime.GOMAXPROCS(runtime.NumCPU())

	hasher := MurMur3Hasher()
	bloomFilter, err := NewBloomFilter(bfElementsCount, 0.001, hasher)
	if err != nil {
		panic(err)
	}

	//init a pool of workers
	pool := NewPool(workersCount, readBufferSize, bloomFilter)
	pool.Start()        // start the workers in the pool
	defer pool.Finish() // in case of early exit

	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var (
		// chunk may end not at the end of a line, and here, we will store the suffix.
		ending []byte
	)

	for {
		// Read a chunk
		buf := make([]byte, chunkSize)
		n, err := file.Read(buf)
		if n == 0 && errors.Is(err, io.EOF) {
			break
		}

		// Find the end of the last complete line
		end := n
		for i := n - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				end = i + 1
				break
			}
		}

		// send data for processing
		pool.Add(append(ending, buf[:end]...))
		ending = buf[end:]
	}

	if len(ending) > 0 {
		pool.Add(ending)
	}

	fmt.Printf("ended at %s\n", time.Since(now).String())
	pool.Finish() // signal worker to stop when ready

	return bloomFilter.Count(), nil
}

func main() {
	uniqueCount, err := CountUniqueIPs(fileName)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("Aproximate Number of unique IP addresses: %d\n", uniqueCount)
}
