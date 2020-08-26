package main

import (
	"context"
	"fmt"
	"log"

	"github.com/csmadhu/gob"
)

func main() {
	g, err := gob.New(gob.WithDBProvider(gob.DBProviderMySQL),
		gob.WithDBConnStr("gob:fY5SGU=t@(localhost:3306)/gob"))
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

	if err := g.Upsert(context.Background(), gob.UpsertArgs{
		Model:          "students",
		Keys:           []string{"name"},
		ConflictAction: gob.ConflictActionUpdate,
		Rows:           rows}); err != nil {
		log.Fatalf("upsert students; err: %v", err)
	}
}
