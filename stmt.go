package zeroxorm

import (
	"database/sql"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"github.com/tal-tech/go-zero/core/timex"
	"github.com/xormplus/xorm"
)

type zeroStmt struct {
	session *xorm.Session
	query   string
}

func newStmtSession(conn *xorm.Engine, query string) sqlx.StmtSession {
	sess := conn.NewSession()
	sess = sess.Prepare()
	return &zeroStmt{sess, query}
}

func (s *zeroStmt) Close() error {
	return s.session.Close()
}

func (s *zeroStmt) Exec(args ...interface{}) (sql.Result, error) {
	return exec(s.session, s.query, args...)
}

func (s *zeroStmt) QueryRow(v interface{}, args ...interface{}) error {
	return query(s.session, v, s.query, args...)
}

func (s *zeroStmt) QueryRowPartial(v interface{}, args ...interface{}) error {
	return s.QueryRow(v, args...)
}

func (s *zeroStmt) QueryRows(v interface{}, args ...interface{}) error {
	return s.QueryRow(v, args...)
}

func (s *zeroStmt) QueryRowsPartial(v interface{}, args ...interface{}) error {
	return s.QueryRows(v, args...)
}

func exec(session *xorm.Session, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := format(query, args...)
	if err != nil {
		return nil, err
	}

	startTime := timex.Now()
	result, err := session.Exec(query, args)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] exec: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql exec: %s", stmt)
	}

	if err != nil {
		logSqlError(stmt, err)
	}

	return result, err
}

func query(session *xorm.Session, v interface{}, query string, args ...interface{}) error {
	stmt, err := format(query, args...)
	if err != nil {
		return err
	}

	startTime := timex.Now()
	err = session.SQL(query, args...).Find(v)
	duration := timex.Since(startTime)
	if duration > slowThreshold {
		logx.WithDuration(duration).Slowf("[SQL] exec: slowcall - %s", stmt)
	} else {
		logx.WithDuration(duration).Infof("sql exec: %s", stmt)
	}

	if err != nil {
		logSqlError(stmt, err)
	}

	return err
}
