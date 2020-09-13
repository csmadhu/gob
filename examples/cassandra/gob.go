package main

import (
	"context"
	"fmt"
	"log"

	"github.com/csmadhu/gob"
)

func main() {
	g, err := gob.New(gob.WithDBProvider(gob.DBProviderCassandra),
		gob.WithDBConnStr("cassandra://localhost:9042/gob?consistency=quorum&compressor=snappy&tokenAware=true"))
	if err != nil {
		log.Fatalf("init gob; err: %v", err)
	}
	defer g.Close()

	// upsert records to table student
	var rows []gob.Row
	for i := 0; i < 10; i++ {
		row := gob.NewRow()
		row.Add("name", fmt.Sprintf("name-%d", i))
		row.Add("age", 20)

		rows = append(rows, row)
	}

	if err := g.Upsert(context.Background(), gob.UpsertArgs{
		Model:          "students",
		ConflictAction: gob.ConflictActionUpdate,
		Rows:           rows}); err != nil {
		log.Fatalf("upsert students; err: %v", err)
	}
}
