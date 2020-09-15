# gob
Bulk upserts to PostgreSQL, MySQL, Cassandra

[![Go Report Card](https://goreportcard.com/badge/github.com/csmadhu/gob)](https://goreportcard.com/report/github.com/csmadhu/gob)
[![GoDoc](https://godoc.org/github.com/csmadhu/gob?status.svg)](https://pkg.go.dev/github.com/csmadhu/gob?tab=doc)
[![Conventional Commit](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)
[![Gitmoji](https://img.shields.io/badge/gitmoji-%20😜%20😍-FFDD67.svg?style=flat-square)](https://gitmoji.carloscuesta.me)
[![Go](https://github.com/csmadhu/gob/workflows/Go/badge.svg)](https://github.com/csmadhu/gob/actions)
![Supported Go Versions](https://img.shields.io/badge/Go-1.15+-lightgrey.svg)
[![GitHub Release](https://img.shields.io/github/release/csmadhu/gob.svg)](https://github.com/csmadhu/gob/releases)
[![License](https://img.shields.io/badge/license-GPL%20(%3E%3D%202)-blue)](https://github.com/csmadhu/gob/blob/master/LICENSE)

---------------------------------------
  * [Installation](#installation)
  * [Usage](#usage)
  * [Options](#options)
  * [Examples](#examples)
---------------------------------------

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

<table>
	<tr>
		<th>option</th>
		<th>description</th>
		<th>type</th>
		<th>default</th>
		<th>DB providers</th>
	</tr>
	<tr>
		<td><b>WithBatchSize</b></th>
		<td>Transaction Batch Size</td>
		<td>int</td>
		<td>10000</td>
		<td><ul><li>PostgreSQL</li><li>MySQL</li></ul></td>
	</tr>
	<tr>
		<td><b>WithDBProvider</b></th>
		<td>Database provider</td>
		<td>gob.DBProvider</td>
		<td>DBProviderPg</td>
		<td>
			<ul>
				<li>PostgreSQL</li>
				<li>MySQL</li>
				<li>Cassandra</li>
			</ul>
		</td>
	</tr>
	<tr>
		<td><b>WithDBConnStr</b></th>
		<td>DB URL/DSN
			<ul>
				<li>PostgreSQL <b>postgres://username:password@host:port/batabase</b><br>
					References
						<ul>
							<li>https://pkg.go.dev/github.com/jackc/pgconn?tab=doc#ParseConfig</li>
							<li>https://pkg.go.dev/github.com/jackc/pgx/v4?tab=doc#ParseConfig</li>
						</ul>
				</li>
				<li>MySQL <b>username:password@(host:port)/database</b></li>
				<li>Cassandra <b><nobr>cassandra://username:password@host1--host2--host3:port/keyspace?consistency=quorum&compressor=snappy&tokenAware=true</nobr></b><br>
					References
						<ul>
						<li>https://godoc.org/github.com/gocql/gocql#Consistency</li>
						<li>https://godoc.org/github.com/gocql/gocql#Compressor</li>
						<li>https://godoc.org/github.com/gocql/gocql#PoolConfig</li>
						<li>https://godoc.org/github.com/gocql/gocql#HostSelectionPolicy</li>
						<li>https://godoc.org/github.com/gocql/gocql#TokenAwareHostPolicy</li>
						</ul>
				</li>
			</ul>
		</td>
		<td>string</td>
		<td><nobr>postgres://postgres:postgres@localhost:5432/gob?pool_max_conns=1</nobr></td>
		<td>
			<ul>
				<li>PostgreSQL</li>
				<li>MySQL</li>
				<li>Cassandra</li>
			</ul>
		</td>
	</tr>
	<tr>
		<td><b>WithConnIdleTime</b></th>
		<td>Maximum amount of time conn may be idle</td>
		<td>time.Duration</td>
		<td>3 second</td>
		<td>
			<ul>
				<li>PostgreSQL</li>
				<li>MySQL</li>
			</ul>
		</td>
	</tr>
	<tr>
		<td><b>WithConnLifeTime</b></th>
		<td>Maximum amount of time conn may be reused</td>
		<td>time.Duration</td>
		<td>3 second</td>
		<td>
			<ul>
				<li>PostgreSQL</li>
				<li>MySQL</li>
				<li>Cassandra</li>
			</ul>
		</td>
	</tr>
	<tr>
		<td><b>WithIdleConns</b></th>
		<td>Maximum number of connections idle in pool</td>
		<td>int</td>
		<td>2</td>
		<td>
			<ul>
				<li>PostgreSQL</li>
				<li>MySQL</li>
			</ul>
		</td>
	</tr>
	<tr>
		<td><b>WithOpenConns</b></th>
		<td>Maximum number of connections open to database</td>
		<td>int</td>
		<td>10</td>
		<td>
			<ul>
				<li>PostgreSQL</li>
				<li>MySQL</li>
				<li>Cassandra</li>
			</ul>
		</td>
	</tr>
</table>

## Examples
Examples for supported Database provider can be found [here](https://github.com/csmadhu/gob/tree/master/examples)
