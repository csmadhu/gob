package gob

import (
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

var (
	testCassyDB         *gocql.Session
	testCassyConnString = "cassandra://localhost:9042/gob?consistency=quorum&compressor=snappy&tokenAware=true"
	testCassyArgs       = connArgs{
		connStr:      testCassyConnString,
		openConns:    1,
		connLifeTime: 3 * time.Second,
	}
)

func init() {
	if err := setupCassyDB(); err != nil {
		log.Fatalf("setup cassandra; err: %v", err)
	}
}

func setupCassyDB() error {
	cluster, err := parseCassyConnString(testCassyArgs.connStr)
	if err != nil {
		return err
	}

	cluster.Keyspace = "system"
	cluster.Timeout = testCassyArgs.connLifeTime
	cluster.NumConns = testCassyArgs.openConns

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect to Cassnadra with keyspace %s: %w", cluster.Keyspace, err)
	}

	var (
		testCassyKeyspace = "gob"
		testCassyTable    = "students"
	)

	// Drop keyspace
	if err := session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testCassyKeyspace)).Exec(); err != nil {
		return fmt.Errorf("drop keyspace %s: %w", testCassyKeyspace, err)
	}

	// create keyspace
	if err = session.Query(fmt.Sprintf(`CREATE KEYSPACE %s
    WITH replication = {
        'class' : 'SimpleStrategy',
        'replication_factor' : %d
    }`, testCassyKeyspace, 1)).Exec(); err != nil {
		return fmt.Errorf("create keyspace %s: %w", testCassyKeyspace, err)
	}

	// drop table
	if err := session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", testCassyKeyspace, testCassyTable)).Exec(); err != nil {
		return fmt.Errorf("drop table %s: %w", testCassyTable, err)
	}

	// create table
	if err := session.Query(fmt.Sprintf(`CREATE TABLE %s.%s( 
		name TEXT PRIMARY KEY, 
		age INT, 
		profile MAP<TEXT, TEXT>, 
		subjects LIST<TEXT>,
		birthday TIMESTAMP
		)`, testCassyKeyspace, testCassyTable)).Exec(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// close session
	session.Close()

	// init session with gob keyspace
	cluster.Keyspace = testCassyKeyspace
	session, err = cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect to Cassnadra with keyspace %s: %w", cluster.Keyspace, err)
	}

	testCassyDB = session
	return nil
}

func testGenStudentRowsCassy(count int) (rows []Row) {
	subjects := []string{"english", "calculus"}
	for i := 0; i < count; i++ {
		row := NewRow()
		row.Add("name", fmt.Sprintf("name-%d", i))
		row.Add("age", i)
		row.Add("profile", map[string]string{
			"state":   fmt.Sprintf("state-%d", i),
			"street":  fmt.Sprintf("street-%d", i),
			"zipcode": fmt.Sprintf("%d", i)})
		row.Add("subjects", subjects)
		row.Add("birthday", time.Now())

		rows = append(rows, row)
	}

	return rows
}

func testVerifyStudentRowsCassy(t *testing.T, rows []Row) {
	for _, want := range rows {
		iter := testCassyDB.Query("SELECT name, age, profile, subjects FROM students WHERE name=?", want.Value("name")).Iter()
		var got = NewRow()

		if iter.NumRows() != 1 {
			t.Fatalf("rowCount got: %d want: %d", iter.NumRows(), 1)
		}

		if !iter.MapScan(got) {
			t.Fatalf("failed to scan row")
		}

		if err := iter.Close(); err != nil {
			t.Fatalf("close iter: %v", err)
		}

		if got.Len() == 0 {
			t.Fatalf("row got: empty row want: %v", want)
		}

		got["birthday"] = want.Value("birthday")

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("student row: %s got: %+v want: %+v", want.Value("name"), got, want)
		}
	}
}

func TestParseCassyConnString(t *testing.T) {
	tests := []struct {
		name       string
		connString string
		valid      bool
	}{
		{
			name:       "invalidConnString",
			connString: "cassy://host:port@username:password/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      false,
		},
		{
			name:       "emptyHost",
			connString: "cassandra://username:password@/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "onlyPort",
			connString: "cassandra://username:password@:9042/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "emptyKeyspace",
			connString: "cassandra://username:password@/?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      false,
		},
		{
			name:       "singleHost",
			connString: "cassandra://username:password@localhost:9042/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "withoutAuth",
			connString: "cassandra://localhost:9042/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "withoutAuthHost",
			connString: "cassandra:///keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "multipleHosts",
			connString: "cassandra://username:password@127.0.0.1--localhost:9042/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "invalidPort",
			connString: "cassandra://username:password@localhost:port/keyspace?consistency=quorum&compressor=snappy&tokenAware=true",
			valid:      false,
		},
		{
			name:       "consistencyThree",
			connString: "cassandra://username:password@localhost:9042/keyspace?consistency=three&compressor=snappy&tokenAware=true",
			valid:      true,
		},
		{
			name:       "invalidConsistency",
			connString: "cassandra://username:password@localhost:9042/keyspace?consistency=invalid&compressor=snappy&tokenAware=true",
			valid:      false,
		},
		{
			name:       "invalidCompressions",
			connString: "cassandra://username:password@localhost:9042/keyspace?consistency=quorum&compressor=gzip&tokenAware=true",
			valid:      false,
		},
		{
			name:       "noTokenaware",
			connString: "cassandra://username:password@localhost:9042/keyspace?consistency=quorum&tokenAware=false",
			valid:      true,
		},
	}

	for _, test := range tests {
		log.Printf("Test [%s]", test.name)
		_, err := parseCassyConnString(test.connString)
		if test.valid && err != nil {
			t.Fatalf("parse conn string: %s err: %v", test.connString, err)
		}

		if !test.valid && err == nil {
			t.Fatalf("parse conn string: %s wanted error", test.connString)
		}
	}
}

func TestNewCassandra(t *testing.T) {
	db, err := newCassandra(testCassyArgs)
	if err != nil {
		t.Fatalf("init Cassandra; err: %v", err)
	}

	db.close()
}

func TestUpsertCassy(t *testing.T) {
	setupCassyDB()
	db, err := newCassandra(testCassyArgs)
	if err != nil {
		t.Fatalf("init Cassandra err: %v", err)
	}
	defer db.close()

	testUpsertDB(t, db, testGenStudentRowsCassy, testVerifyStudentRowsCassy)
}

func TestRowToCQL(t *testing.T) {
	wantSQLs := []string{
		"INSERT INTO students (age,birthday,name,profile,subjects) VALUES(?,?,?,?,?)",
		"INSERT INTO students (age,birthday,name,profile,subjects) VALUES(?,?,?,?,?) IF NOT EXISTS",
	}

	wantArgs := [][]interface{}{
		{0, nil, "name-0", map[string]string{"street": "street-0", "state": "state-0", "zipcode": "0"}, []string{"english", "calculus"}},
		{0, nil, "name-0", map[string]string{"street": "street-0", "state": "state-0", "zipcode": "0"}, []string{"english", "calculus"}},
	}

	cassy := &cassy{}
	testRowToSQL(t, testGenStudentRowsCassy, cassy.rowToCQL, wantSQLs, wantArgs)
}
