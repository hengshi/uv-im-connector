package lark

import (
	"strconv"
	"sync"
	"time"
)

type chunkAssembler struct {
	ttl time.Duration
	now func() time.Time

	mu  sync.Mutex
	buf map[string]*chunkEntry
}

type chunkEntry struct {
	chunks   map[int][]byte
	sum      int
	deadline time.Time
}

func newChunkAssembler(ttl time.Duration, now func() time.Time) *chunkAssembler {
	if now == nil {
		now = time.Now
	}
	return &chunkAssembler{ttl: ttl, now: now, buf: map[string]*chunkEntry{}}
}

func (a *chunkAssembler) admit(messageID string, sum, seq int, payload []byte) ([]byte, bool) {
	if messageID == "" || sum <= 0 || seq < 0 || seq >= sum {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gcExpiredLocked()
	entry, ok := a.buf[messageID]
	if !ok {
		entry = &chunkEntry{chunks: make(map[int][]byte), sum: sum}
		a.buf[messageID] = entry
	}
	if entry.sum != sum {
		return nil, false
	}
	entry.chunks[seq] = append([]byte(nil), payload...)
	if a.ttl > 0 {
		entry.deadline = a.now().Add(a.ttl)
	}
	if len(entry.chunks) < entry.sum {
		return nil, false
	}
	total := 0
	for _, chunk := range entry.chunks {
		total += len(chunk)
	}
	out := make([]byte, 0, total)
	for seq := 0; seq < entry.sum; seq++ {
		out = append(out, entry.chunks[seq]...)
	}
	delete(a.buf, messageID)
	return out, true
}

func (a *chunkAssembler) gcExpiredLocked() {
	now := a.now()
	for id, entry := range a.buf {
		if !entry.deadline.IsZero() && now.After(entry.deadline) {
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
