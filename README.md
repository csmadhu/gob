# gob
Bulk upserts to PostgreSQL, MySQL, Cassandra
<p align="left">
	<a href="https://goreportcard.com/report/github.com/csmadhu/gob"><img src="https://goreportcard.com/badge/github.com/csmadhu/gob"/></a>
	<a href="https://pkg.go.dev/github.com/csmadhu/gob?tab=doc"><img src="https://godoc.org/github.com/csmadhu/gob?status.svg"/></a>
	<a href="https://conventionalcommits.org"><img src="https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg"/></a>
	<a href="/.github/workflows/go.yml"><img src="https://github.com/csmadhu/gob/workflows/Go/badge.svg"/></a>
	<a href="https://gitmoji.carloscuesta.me"><img src="https://img.shields.io/badge/gitmoji-%20😜%20😍-FFDD67.svg?style=flat-square" alt="Gitmoji"></a>
	<a href="/LICENSE"><img src="https://img.shields.io/badge/license-GPL%20(%3E%3D%202)-blue" alt="license"/></a>
</p>

---------------------------------------
  * [Requirements](#requirements)
  * [Installation](#installation)
  * [Usage](#usage)
  * [Options](#options)
  * [Examples](#examples)
---------------------------------------

## Requirements
* GO 1.15 and above

## Installation
```bash
go get -u github.com/csmadhu/gob
```

# Usage
```go
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

	if err := g.Upsert(context.Background(), gob.UpsertArgs{
		Model:          "students",
		Keys:           []string{"name"},
		ConflictAction: gob.ConflictActionUpdate,
		Rows:           rows}); err != nil {
		log.Fatalf("upsert students; err: %v", err)
	}
}
```

## Options
All options are optional. Options not applicable to Database provider is ignored.

| option | description | type | default |
|-------------|-------------|-------|-------|
| **WithBatchSize** | Transaction Batch Size | int | 10000 |
| **WithDBProvider** | Database provider | gob.DBProvider | DBProviderPg |
| **WithDBConnStr** | DB URL/DSN<ul><li>PostgreSQL <i>postgres://username:password@host:port/batabase</i><br>References<ul><li>https://pkg.go.dev/github.com/jackc/pgconn?tab=doc#ParseConfig</li><li>https://pkg.go.dev/github.com/jackc/pgx/v4?tab=doc#ParseConfig</li></ul><li>MySQL <i>username:password@(host:port)/database</i></li><li>Cassandra <i>cassandra://username:password@host1--host2--host3:port/keyspace?consistency=quorum&compressor=snappy&tokenAware=true</i><br>References<ul><li>https://godoc.org/github.com/gocql/gocql#Consistency</li><li>https://godoc.org/github.com/gocql/gocql#Compressor</li><li>https://godoc.org/github.com/gocql/gocql#PoolConfig</li><li>https://godoc.org/github.com/gocql/gocql#HostSelectionPolicy</li><li>https://godoc.org/github.com/gocql/gocql#TokenAwareHostPolicy</li></ul></li></ul>| string | postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1 |
| **WithConnIdleTime**  | Maximum amount of time conn may be idle | time.Duration | 3 second |
| **WithConnLifeTime**  | Maximum amount of time conn may be reused | time.Duration | 3 second |
| **WithIdleConns** | Maximum number of connections idle in pool | int | 2 |
| **WithOpenConns** | Maximum number of connections open to database | int | 10 |

## Examples
Examples for supported Database provider can be found [here](https://github.com/csmadhu/gob/tree/master/examples)
