package main

import (
	"hash"

	"github.com/spaolacci/murmur3"
)

// Mur3Hasher has holder.
type Mur3Hasher struct{}

// MurMur3Hasher creates new hash holder
func MurMur3Hasher() *Mur3Hasher {
	return &Mur3Hasher{}
}

// GetHashes generate `n` hash functions index as a seed.
func (h *Mur3Hasher) GetHashes(n uint64) []hash.Hash64 {
	hashers := make([]hash.Hash64, n)
	for i := 0; uint64(i) < n; i++ {
		hashers[i] = murmur3.New64WithSeed(uint32(i))
	}

	return hashers
}
