package gob

import (
	"testing"
)

func TestNewGob(t *testing.T) {
	g := gob()
	if g.batchSize != defaultBatchSize {
		t.Fatalf("batchSize got: %d want: %d", g.batchSize, defaultBatchSize)
	}

	batchSize := 10
	g = gob(BatchSize(batchSize))
	if g.batchSize != batchSize {
		t.Fatalf("batchSize got: %d want: %d", g.batchSize, batchSize)
	}
}
