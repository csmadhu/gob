package gob

var (
	defaultBatchSize = 10000
)

// Gob provides APIs to upsert data in bulk
type Gob struct {
	db        // connection handler to database
	batchSize int
}

func gob(options ...Option) *Gob {
	g := &Gob{batchSize: defaultBatchSize}

	for _, option := range options {
		option(g)
	}

	return g
}

func (gob *Gob) setBatchSize(size int) {
	gob.batchSize = size
}

func (gob *Gob) setDB(db db) {
	gob.db = db
}

// Option to customize Gob
type Option func(gob *Gob)

// BatchSize sets batchSize of Gob to size
func BatchSize(size int) Option {
	return func(gob *Gob) {
		gob.setBatchSize(size)
	}
}

// Row of model
type Row map[string]interface{}

type db interface {
	// Insert rows to model
	Insert(model string, rows []Row) error

	// Update rows to model
	Update(model string, rows []Row) error
}
