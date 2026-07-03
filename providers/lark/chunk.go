package lark

import (
	"strconv"
	"sync"
	"time"
)

const (
	maxChunkSum            = 256
	maxAssembledChunkBytes = 100 * 1024 * 1024
)

type chunkAssembler struct {
	ttl time.Duration
	now func() time.Time

	mu  sync.Mutex
	buf map[string]*chunkEntry
}

type chunkEntry struct {
	chunks   [][]byte
	received int
	size     int
	deadline time.Time
}

func newChunkAssembler(ttl time.Duration, now func() time.Time) *chunkAssembler {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &chunkAssembler{ttl: ttl, now: now, buf: map[string]*chunkEntry{}}
}

func (a *chunkAssembler) admit(messageID string, sum, seq int, payload []byte) ([]byte, bool) {
	if messageID == "" || sum <= 0 || sum > maxChunkSum || seq < 0 || seq >= sum || len(payload) > maxAssembledChunkBytes {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gcExpiredLocked()
	entry, ok := a.buf[messageID]
	if !ok {
		entry = &chunkEntry{chunks: make([][]byte, sum), deadline: a.now().Add(a.ttl)}
		a.buf[messageID] = entry
	}
	if entry.chunks[seq] == nil {
		entry.received++
		entry.size += len(payload)
	} else {
		entry.size -= len(entry.chunks[seq])
		entry.size += len(payload)
	}
	if entry.size > maxAssembledChunkBytes {
		delete(a.buf, messageID)
		return nil, false
	}
	entry.chunks[seq] = append([]byte(nil), payload...)
	entry.deadline = a.now().Add(a.ttl)
	if entry.received < len(entry.chunks) {
		return nil, false
	}
	total := 0
	for _, chunk := range entry.chunks {
		total += len(chunk)
	}
	out := make([]byte, 0, total)
	for _, chunk := range entry.chunks {
		out = append(out, chunk...)
	}
	delete(a.buf, messageID)
	return out, true
}

func (a *chunkAssembler) gcExpiredLocked() {
	now := a.now()
	for id, entry := range a.buf {
		if now.After(entry.deadline) {
			delete(a.buf, id)
		}
	}
}

func parseChunkHeaders(f *wsFrame) (sum, seq int, messageID string) {
	if f == nil {
		return 0, 0, ""
	}
	if value := f.headerValue(frameHeaderSum); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			sum = n
		}
	}
	if value := f.headerValue(frameHeaderSeq); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			seq = n
		}
	}
	return sum, seq, f.headerValue(frameHeaderMessageID)
}
