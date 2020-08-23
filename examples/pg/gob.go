package main

import (
	"context"
	"log"

	"github.com/csmadhu/gob"
)

func main() {
	g, err := gob.New(gob.DBType(gob.DBTypePg),
		gob.DBConnStr("postgres://postgres:postgres@localhost:5432/postgres?pool_max_conns=1"))
	if err != nil {
		log.Fatalf("init gob err: %v", err)
	}
	defer g.Close()

	// upsert records to table foo
	var rows []gob.Row
	for i := 0; i < 10; i++ {
		row := gob.NewRow()
		row.Add("id", i)
		row.Add("name", "foo")

		rows = append(rows, row)
	}

	if err := g.Upsert(context.Background(), "student", []string{"id"}, rows); err != nil {
		log.Fatalf("upsert student rows err: %v", err)
	}
}
