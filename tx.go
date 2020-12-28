package zeroxorm

import (
	"database/sql"
	"fmt"

	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"github.com/xormplus/xorm"
)

type (
	beginnable func(*xorm.Engine) (trans, error)

	trans interface {
		sqlx.Session
		Commit() error
		Rollback() error
		Close() error
	}

	txSession struct {
		*xorm.Session
	}
)

func (t txSession) Exec(q string, args ...interface{}) (sql.Result, error) {
	return exec(t.Session, q, args...)
}

func (t txSession) Prepare(q string) (sqlx.StmtSession, error) {
	stmt := &zeroStmt{t.Session, q}
	return stmt, nil
}

func (t txSession) QueryRow(v interface{}, q string, args ...interface{}) error {
	return query(t.Session, v, q, args...)
}

func (t txSession) QueryRowPartial(v interface{}, q string, args ...interface{}) error {
	return query(t.Session, v, q, args...)
}

func (t txSession) QueryRows(v interface{}, q string, args ...interface{}) error {
	return query(t.Session, v, q, args...)
}

func (t txSession) QueryRowsPartial(v interface{}, q string, args ...interface{}) error {
	return query(t.Session, v, q, args...)
}

func (t txSession) Commit() error {
	return t.Session.Commit()
}

func (t txSession) Rollback() error {
	return t.Session.Rollback()
}

func (t txSession) Close() error {
	return t.Session.Close()
}

func begin(db *xorm.Engine) (trans, error) {
	sess := db.NewSession()
	sess = sess.Prepare()
	err := sess.Begin()
	if err != nil {
		return nil, err
	}

	return txSession{sess}, nil
}

func transactOnConn(conn *xorm.Engine, b beginnable, fn func(sqlx.Session) error) (err error) {
	var tx trans
	tx, err = b(conn)
	if err != nil {
		return
	}

	defer tx.Close()
	defer func() {
		if p := recover(); p != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("recover from %#v, rollback failed: %s", p, e)
			} else {
				err = fmt.Errorf("recoveer from %#v", p)
			}
		} else if err != nil {
			if e := tx.Rollback(); e != nil {
				err = fmt.Errorf("transaction failed: %s, rollback failed: %s", err, e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	return fn(tx)
}
