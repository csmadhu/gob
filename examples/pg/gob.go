package main

import (
	"context"
	"fmt"
	"log"

	"github.com/csmadhu/gob"
)

func main() {
	g, err := gob.New(gob.WithDBProvider(gob.DBProviderPg),
		gob.WithDBConnStr("postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1"))
	if err != nil {
		log.Fatalf("init gob; err: %v", err)
	}
	defer g.Close()

	// upsert records to table student
	var rows []gob.Row
	for i := 0; i < 10; i++ {
		row := gob.NewRow()
		row.Add("name", fmt.Sprintf("foo-%d", i))
		row.Add("age", 20)

		rows = append(rows, row)
	}

	if err := g.Upsert(context.Background(), "students", []string{"name"}, gob.ConflictActionUpdate, rows); err != nil {
		log.Fatalf("upsert students; err: %v", err)
	}
}
