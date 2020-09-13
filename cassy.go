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
		u                        *url.URL
		config                   *gocql.ClusterConfig
		hosts                    []string
		host, username, password string
		err                      error
	)

	u, err = url.Parse(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}

	// parse hosts
	host = u.Hostname()
	switch {
	case host == "":
		hosts = []string{"localhost"}
	default:
		hosts = strings.Split(host, "--")
	}

	config = gocql.NewCluster(hosts...)

	// parse keyspace
	config.Keyspace = strings.TrimLeft(u.Path, "/")
	if config.Keyspace == "" {
		return nil, ErrEmptyKeyspace
	}

	// parse port
	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("parse port %s: %w", p, err)
		}
		config.Port = port
	}

	// parse username password
	username = u.User.Username()
	password, _ = u.User.Password()
	if username != "" {
		config.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	for setting, values := range u.Query() {
		switch setting {
		case "consistency":
			consistency, err := gocql.ParseConsistencyWrapper(values[0])
			if err != nil {
				return nil, fmt.Errorf("parse consistency %s: %w", values[0], err)
			}
			config.Consistency = consistency
		case "compressor":
			if strings.ToLower(values[0]) != cassyCompression {
				return nil, fmt.Errorf("invalid compression: %s; valid compression: %s", values[0], cassyCompression)
			}
			config.Compressor = gocql.SnappyCompressor{}
		case "tokenAware":
			if strings.ToLower(values[0]) != "true" {
				continue
			}
			config.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
		}
	}

	return config, nil
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
