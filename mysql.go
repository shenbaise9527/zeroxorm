package zeroxorm

import (
	"github.com/go-sql-driver/mysql"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

const (
	duplicateEntryCode uint16 = 1062
)

// NewZeroMysqlConn use xorm to bridge go-zero.
func NewZeroMysqlConn(datasource string, opts ...ZeroxormOption) sqlx.SqlConn {
	opts = append(opts, withMysqlAcceptable())
	return NewZeroxormConn("mysql", datasource, opts...)
}

func mysqlAcceptable(err error) bool {
	if err == nil {
		return true
	}

	myerr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch myerr.Number {
	case duplicateEntryCode:
		return true
	default:
		return false
	}
}

func withMysqlAcceptable() ZeroxormOption {
	return func(conn *zeroxormConn) {
		conn.accept = mysqlAcceptable
	}
}
