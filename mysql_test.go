package gob

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

var (
	testMySQLDB      *sql.DB
	testMySQLConnStr = "gob:fY5SGU=t@(localhost:3306)/gob"
	testMySQLArgs    = connArgs{
		connStr:      testMySQLConnStr,
		idleConns:    1,
		openConns:    1,
		connIdleTime: 3 * time.Second,
		connLifeTime: 3 * time.Second,
	}
)

func init() {
	if err := setupMySQLDB(); err != nil {
		log.Fatalf("setup mysql; err: %v", err)
	}
}

func setupMySQLDB() error {
	mysqlDB, err := sql.Open("mysql", testMySQLConnStr)
	if err != nil {
		return fmt.Errorf("connect to MySQL: %w", err)
	}

	if _, err := mysqlDB.Exec("DROP TABLE IF EXISTS students"); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	if _, err := mysqlDB.Exec(`CREATE TABLE students(
		id SERIAL PRIMARY KEY, 
		name VARCHAR(255) NOT NULL, 
		age INT, 
		profile JSON, 
		subjects JSON,
		birthday TIMESTAMP,
		UNIQUE (name)
		)`); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	testMySQLDB = mysqlDB
	return nil
}

func testGenStudentRowsMySQL(count int) (rows []Row) {
	subjects := `["english", "calculus"]`
	profile := `{"state": "state-%d", "street": "street-%d", "zipcode": %d}`
	for i := 0; i < count; i++ {
		row := NewRow()
		row.Add("name", fmt.Sprintf("name-%d", i))
		row.Add("age", i)
		row.Add("profile", fmt.Sprintf(profile, i, i, i))
		row.Add("subjects", subjects)
		row.Add("birthday", time.Now())

		rows = append(rows, row)
	}

	return rows
}

func testVerifyStudentRowsMySQL(t *testing.T, rows []Row) {
	for _, want := range rows {
		dbRows, err := testMySQLDB.Query("SELECT name, age, profile, subjects FROM students WHERE name=?", want.Value("name"))
		if err != nil {
			t.Fatalf("read student row: %s; err: %v", want.Value("name"), err)
		}
		defer dbRows.Close()

		var got Row
		for dbRows.Next() {
			var (
				name     string
				age      int
				profile  string
				subjects string
			)
			if err := dbRows.Scan(&name, &age, &profile, &subjects); err != nil {
				t.Fatalf("scan student row: %s; err: %v", want.Value("name"), err)
			}
			got = NewRow()
			got.Add("name", name)
			got.Add("age", age)
			got.Add("profile", profile)
			got.Add("subjects", subjects)
		}

		if got == nil || got.Len() == 0 {
			t.Fatalf("row got: empty row want: %v", want)
		}

		got["birthday"] = want.Value("birthday")

		if err := dbRows.Err(); err != nil {
			t.Fatalf("scan student rows; err: %v", err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("student row: %s got: %+v want: %+v", want.Value("name"), got, want)
		}
	}
}

func TestNewMySQL(t *testing.T) {
	db, err := newMySQL(testMySQLArgs)
	if err != nil {
		t.Fatalf("init MySQL server; err: %v", err)
	}

	db.close()
}

func TestUpsertMySQL(t *testing.T) {
	setupMySQLDB()
	db, err := newMySQL(testMySQLArgs)
	if err != nil {
		t.Fatalf("init PostgreSQL server err: %v", err)
	}
	defer db.close()

	testUpsertDB(t, db, testGenStudentRowsMySQL, testVerifyStudentRowsMySQL)
}

func TestRowToSQLMySQL(t *testing.T) {
	wantSQLs := []string{
		"INSERT INTO students(age,birthday,name,profile,subjects) VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE age=?,birthday=?,name=?,profile=?,subjects=?",
		"INSERT IGNORE INTO students(age,birthday,name,profile,subjects) VALUES(?,?,?,?,?)",
	}

	wantArgs := [][]interface{}{
		[]interface{}{0, nil, "name-0", `{"state": "state-0", "street": "street-0", "zipcode": 0}`, `["english", "calculus"]`, 0, nil, "name-0", `{"state": "state-0", "street": "street-0", "zipcode": 0}`, `["english", "calculus"]`},
		[]interface{}{0, nil, "name-0", `{"state": "state-0", "street": "street-0", "zipcode": 0}`, `["english", "calculus"]`},
	}

	m := &mysql{}
	testRowToSQL(t, testGenStudentRowsMySQL, m.rowToSQL, wantSQLs, wantArgs)
}
