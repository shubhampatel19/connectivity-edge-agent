package agent

import (
	"sync"

	"github.com/spr732/connectivity-contracts/wire"
)

// Buffer is a bounded ring of samples waiting to be sent. When the backend is
// unreachable long enough to fill it, the oldest samples are overwritten: on a
// device with fixed memory, recent data is worth more than old data.
type Buffer struct {
	mu    sync.Mutex
	items []wire.Sample
	head  int // index of the oldest sample
	size  int
}

func NewBuffer(capacity int) *Buffer {
	if capacity < 1 {
		capacity = 1
	}
	return &Buffer{items: make([]wire.Sample, capacity)}
}

// Push appends samples and returns how many old samples were overwritten.
func (b *Buffer) Push(samples ...wire.Sample) (dropped int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	capacity := len(b.items)
	for _, s := range samples {
		if b.size == capacity {
			b.head = (b.head + 1) % capacity
			b.size--
			dropped++
		}
		b.items[(b.head+b.size)%capacity] = s
		b.size++
	}
	return dropped
}

// Take removes and returns up to n of the oldest samples.
func (b *Buffer) Take(n int) []wire.Sample {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.size {
		n = b.size
	}
	out := make([]wire.Sample, n)
	capacity := len(b.items)
	for i := range out {
		out[i] = b.items[(b.head+i)%capacity]
	}
	b.head = (b.head + n) % capacity
	b.size -= n
	return out
}

func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}
