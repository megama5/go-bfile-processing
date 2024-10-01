package main

import (
	"sync"
)

// Worker describes the interface for a worker
type Worker interface {
	Start()
}

// WorkerConstructor describes the function signature for a worker constructor
type WorkerConstructor func(
	in chan []byte,
	done chan struct{},
	wg *sync.WaitGroup,
	bf BloomFilterInterface,
) Worker

// Pool workers pool
type Pool struct {
	workersCount int           // number of workers for string processing
	in           chan []byte   // input channel for read bytes from file
	done         chan struct{} // stop channel
	wg           sync.WaitGroup

	newWorker      WorkerConstructor
	workersClosers []chan struct{}

	finished bool // prevent duplicate finish call in case of error/panic

	mu sync.Mutex
	bf BloomFilterInterface
}

// NewPool creates new pool.
func NewPool(workersCount, readBufferSize int, bf BloomFilterInterface) *Pool {
	return &Pool{
		workersCount: workersCount,
		in:           make(chan []byte, readBufferSize),
		done:         make(chan struct{}),
		wg:           sync.WaitGroup{},

		newWorker:      NewIPParser,
		workersClosers: make([]chan struct{}, 0, workersCount),
		bf:             bf,
	}
}

// Start starts workers and inits their closers
func (p *Pool) Start() {
	for i := 0; i < p.workersCount; i++ {
		p.wg.Add(1)

		closer := make(chan struct{})
		p.workersClosers = append(p.workersClosers, closer)

		go func() {
			p.newWorker(p.in, closer, &p.wg, p.bf).Start()
		}()
	}
}

// Add adds chunk to the processing channel
func (p *Pool) Add(data []byte) {
	p.in <- data
}

// Finish stops workers and closes channels
func (p *Pool) Finish() {
	if p.finished {
		return
	}

	defer close(p.done)
	defer close(p.in)

	// send signals to all workers to stop
	for i := 0; i < len(p.workersClosers); i++ {
		p.workersClosers[i] <- struct{}{}
		defer close(p.workersClosers[i])
	}

	p.wg.Wait()
	p.finished = true
	println("All workers finished.")
}
