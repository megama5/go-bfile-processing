package main

import (
	"bufio"
	"bytes"
	"sync"
)

// IPParser worker for parsing IPv4 from the string
type IPParser struct {
	in   chan []byte   // the channel to read from
	done chan struct{} // done channel
	wg   *sync.WaitGroup

	stopSignal bool // if set, a worker will finish processing, when there is no data available in `in` channel.

	bf BloomFilterInterface // Bloom filter
}

// NewIPParser constructor.
func NewIPParser(in chan []byte, done chan struct{}, wg *sync.WaitGroup, bf BloomFilterInterface) Worker {
	return &IPParser{
		in:   in,
		done: done,
		wg:   wg,
		bf:   bf,
	}
}

// Start starts the worker.
func (w *IPParser) Start() {
	for {
		select {
		case data := <-w.in:
			w.processData(data)
		case <-w.done:
			w.stopSignal = true
		default:
			if w.stopSignal && len(w.in) == 0 {
				w.wg.Done()
				return
			}
		}
	}
}

// processData processes the data threw the filter.
func (w *IPParser) processData(data []byte) {
	scanner := bufio.NewScanner(bytes.NewBuffer(data))
	for scanner.Scan() {
		ip := scanner.Text()
		if len(ip) == 0 {
			continue
		}

		if !w.bf.Test([]byte(ip)) {
			w.bf.Add([]byte(ip))
		}
	}
}
