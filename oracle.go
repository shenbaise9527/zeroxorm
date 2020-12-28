package zeroxorm

import "github.com/tal-tech/go-zero/core/stores/sqlx"

// NewZeroOracleConn use xorm to bridge go-zero.
func NewZeroOracleConn(datasource string, opts ...ZeroxormOption) sqlx.SqlConn {
	opts = append(opts, withOracleAcceptable())
	return NewZeroxormConn("oci8", datasource, opts...)
}

func oracleAcceptable(err error) bool {
	return err == nil
}

func withOracleAcceptable() ZeroxormOption {
	return func(conn *zeroxormConn) {
		conn.accept = oracleAcceptable
	}
}
