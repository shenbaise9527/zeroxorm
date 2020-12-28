package zeroxorm

import (
	"database/sql"
	"time"

	"github.com/tal-tech/go-zero/core/breaker"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"github.com/xormplus/xorm"
)

type (
	zeroxormConn struct {
		driverName string
		dataSource string
		beginTx    beginnable
		brk        breaker.Breaker
		accept     func(error) bool
	}

	ZeroxormOption func(*zeroxormConn)
)

const slowThreshold = time.Millisecond * 500

func NewZeroxormConn(driverName, dataSource string, opts ...ZeroxormOption) sqlx.SqlConn {
	conn := &zeroxormConn{
		driverName: driverName,
		dataSource: dataSource,
		beginTx:    begin,
		brk:        breaker.NewBreaker(),
	}

	for _, opt := range opts {
		opt(conn)
	}

	return conn
}

func (db *zeroxormConn) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *xorm.Engine
		conn, err = getSqlConn(db.driverName, db.dataSource)
		if err != nil {
			logInstanceError(db.dataSource, err)

			return err
		}

		sess := conn.Prepare()
		result, err = exec(sess, query, args...)
		return err
	}, db.acceptable)

	return
}

func (db *zeroxormConn) Prepare(query string) (stmt sqlx.StmtSession, err error) {
	err = db.brk.DoWithAcceptable(func() error {
		var conn *xorm.Engine
		conn, err = getSqlConn(db.driverName, db.dataSource)
		if err != nil {
			logInstanceError(db.dataSource, err)

			return err
		}

		stmt = newStmtSession(conn, query)
		return nil
	}, db.acceptable)

	return
}

func (db *zeroxormConn) QueryRow(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zeroxormConn) QueryRowPartial(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zeroxormConn) QueryRows(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zeroxormConn) QueryRowsPartial(v interface{}, query string, args ...interface{}) error {
	return db.queryRows(v, query, args...)
}

func (db *zeroxormConn) Transact(fn func(session sqlx.Session) error) error {
	return db.brk.DoWithAcceptable(func() error {
		conn, err := getSqlConn(db.driverName, db.dataSource)
		if err != nil {
			logInstanceError(db.dataSource, err)

			return err
		}

		return transactOnConn(conn, db.beginTx, fn)
	}, db.acceptable)
}

func (db *zeroxormConn) acceptable(err error) bool {
	ok := err == nil || err == sql.ErrNoRows || err == sql.ErrTxDone
	if db.accept == nil {
		return ok
	} else {
		return ok || db.accept(err)
	}
}

func (db *zeroxormConn) queryRows(v interface{}, q string, args ...interface{}) error {
	return db.brk.DoWithAcceptable(func() error {
		conn, err := getSqlConn(db.driverName, db.dataSource)
		if err != nil {
			logInstanceError(db.dataSource, err)
			return err
		}

		sess := conn.Prepare()
		return query(sess, v, q, args...)
	}, func(err error) bool {
		return db.acceptable(err)
	})
}
