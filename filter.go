package main

import (
	"fmt"
	"hash"
	"math"
	"sync"
)

// BloomFilterInterface describes Bloom Filter methods.
type BloomFilterInterface interface {
	Add([]byte)
	Test([]byte) bool
}

// Hasher describes hasher.
type Hasher interface {
	GetHashes(n uint64) []hash.Hash64
}

// BloomFilter represents a single Bloom filter structure.
type BloomFilter struct {
	bitSet []bool        // actual filter
	m      uint64        // filter len
	k      uint64        // hash functions count
	count  uint64        // new entries
	hashes []hash.Hash64 // a list with hash functions
	mutex  sync.Mutex
}

// NewBloomFilter creates a new Bloom filter with the given number.
//
//	elementNumbers - element number. Should be as much Accurate as possible.
//	fpRate - false positive rate. Value between 0 and 1.
func NewBloomFilter(elementNumbers uint64, fpRate float64, hasher Hasher) (*BloomFilter, error) {
	if elementNumbers == 0 {
		return nil, fmt.Errorf("number of elements must be grater than zero")
	}

	if fpRate <= 0 || fpRate >= 1 {
		return nil, fmt.Errorf("false positive rate value must be between 0 and 1")
	}

	if hasher == nil {
		return nil, fmt.Errorf("hasher cannot be nil")
	}

	m, k := getOptimalParams(elementNumbers, fpRate)
	return &BloomFilter{
		m:      m,
		k:      k,
		bitSet: make([]bool, m),
		hashes: hasher.GetHashes(k),
	}, nil
}

// getOptimalParams calculates the optimal parameters for the Bloom filter.
//
//	Returns bit set size and hash functions count
func getOptimalParams(n uint64, p float64) (uint64, uint64) {
	// bit set size
	m := uint64(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	if m == 0 {
		m = 1
	}

	// hash functions count
	k := uint64(math.Ceil((float64(m) / float64(n)) * math.Log(2)))
	if k == 0 {
		k = 1
	}

	return m, k
}

// Add adds an item to the Bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	for _, hashFunc := range bf.hashes {
		hashFunc.Reset()
		if n, err := hashFunc.Write(data); err != nil || n != len(data) {
			panic(fmt.Sprintf("hashFunc.Write(...) returned %d bytes, error: %v", n, err))
		}
		hashValue := hashFunc.Sum64() % bf.m
		bf.bitSet[hashValue] = true
	}
	bf.count++
}

// Test tests if an item might exists in the filter.
func (bf *BloomFilter) Test(data []byte) bool {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	for _, hashFunc := range bf.hashes {
		hashFunc.Reset()
		if n, err := hashFunc.Write(data); err != nil || n != len(data) {
			panic(fmt.Sprintf("hashFunc.Write(...) returned %d bytes, error: %v", n, err))
		}
		hashValue := hashFunc.Sum64() % bf.m
		if !bf.bitSet[hashValue] {
			return false
		}
	}
	return true
}

// Count gets approximate unique record count.
func (bf *BloomFilter) Count() uint64 {
	return bf.count
}
