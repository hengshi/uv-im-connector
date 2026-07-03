package conformance

import (
	"testing"

	"github.com/hengshi/uv-im-connector/providers/memory"
)

func TestMemoryProviderConformance(t *testing.T) {
	AssertProvider(t, memory.New("memory"))
}
