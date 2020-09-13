package gob

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

const (
	cassyConnStringFmt = "cassandra://username:password@host1--host2--host3:port/keyspace?consistency=quorum&compressor=snappy&tokenAware=true"
	cassyCompression   = "snappy"
)

type cassy struct {
	*gocql.Session
}

func newCassandra(args connArgs) (db, error) {
	var (
		c       cassy
		err     error
		cluster *gocql.ClusterConfig
	)

	cluster, err = parseCassyConnString(args.connStr)
	if err != nil {
		return nil, err
	}

	cluster.Timeout = args.connLifeTime
	cluster.NumConns = args.openConns

	c.Session, err = cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("gob: connect to Cassandra: %w", err)
	}

	return &c, nil
}

func parseCassyConnString(connString string) (*gocql.ClusterConfig, error) {
	if !strings.HasPrefix(connString, "cassandra://") {
		return nil, fmt.Errorf("invalid format: %s; valid format: %s", connString, cassyConnStringFmt)
	}

	var (
		u      *url.URL
		config *gocql.ClusterConfig
		err    error
	)

	u, err = url.Parse(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}

	config = gocql.NewCluster(cassyHosts(u)...)

	// parse keyspace
	config.Keyspace = strings.TrimLeft(u.Path, "/")
	if config.Keyspace == "" {
		return nil, ErrEmptyKeyspace
	}

	config.Port, err = cassyPort(u)
	if err != nil {
		return nil, err
	}

	config.Authenticator = cassyAuth(u)

	config.Consistency, err = cassyConsistency(u.Query().Get("consistency"))
	if err != nil {
		return nil, err
	}

	config.Compressor, err = cassyCompressor(u.Query().Get("compressor"))
	if err != nil {
		return nil, err
	}

	config.PoolConfig.HostSelectionPolicy = cassyHostSelectionPolicy(u.Query().Get("tokenAware"))

	return config, nil
}

// parse hosts
func cassyHosts(u *url.URL) (hosts []string) {
	host := u.Hostname()
	switch {
	case host == "":
		hosts = []string{"localhost"}
	default:
		hosts = strings.Split(host, "--")
	}

	return hosts
}

// parse port
func cassyPort(u *url.URL) (port int, err error) {
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return 0, fmt.Errorf("parse port %s: %w", p, err)
		}
	}

	return port, nil
}

// parse username password
func cassyAuth(u *url.URL) gocql.Authenticator {
	username := u.User.Username()
	password, _ := u.User.Password()
	if username != "" {
		return gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	return nil
}

func cassyConsistency(value string) (gocql.Consistency, error) {
	if value == "" {
		return gocql.Quorum, nil
	}

	consistency, err := gocql.ParseConsistencyWrapper(value)
	if err != nil {
		return gocql.Quorum, fmt.Errorf("parse consistency %s: %w", value, err)
	}

	return consistency, nil
}

func cassyCompressor(value string) (gocql.Compressor, error) {
	if value == "" {
		return nil, nil
	}

	if strings.ToLower(value) != cassyCompression {
		return nil, fmt.Errorf("invalid compression: %s; valid compression: %s", value, cassyCompression)
	}

	return gocql.SnappyCompressor{}, nil
}

func cassyHostSelectionPolicy(value string) gocql.HostSelectionPolicy {
	if strings.ToLower(value) != "true" {
		return gocql.RoundRobinHostPolicy()
	}

	return gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
}

func (db *cassy) upsert(ctx context.Context, upsertArgs UpsertArgs) error {
	if len(upsertArgs.Rows) == 0 {
		return nil
	}

	for _, row := range upsertArgs.Rows {
		if row.Len() == 0 {
			continue // ignore empty row
		}

		sql, args := db.rowToCQL(row, upsertArgs)
		if err := db.Query(sql, args...).Exec(); err != nil {
			return fmt.Errorf("gob: execute sql '%s' on Cassandra: %w", sql, err)
		}
	}

	return nil
}

func (db *cassy) rowToCQL(row Row, upsertArgs UpsertArgs) (sql string, args []interface{}) {
	upsertSQL := "INSERT INTO %s (%s) VALUES(%s) %s"

	var (
		cols   []string
		values []string
		action string
	)

	for _, column := range row.Columns() {
		cols = append(cols, column)
		values = append(values, "?")
		args = append(args, row.Value(column))
	}

	switch upsertArgs.ConflictAction {
	case ConflictActionUpdate:
		action = ""
	case ConflictActionNothing:
		action = "IF NOT EXISTS"
	}

	sql = fmt.Sprintf(upsertSQL,
		upsertArgs.Model,
		strings.Join(cols, ","),
		strings.Join(values, ","),
		action,
	)

	return strings.TrimSpace(sql), args
}

func (db *cassy) close() {
	db.Close()
}
