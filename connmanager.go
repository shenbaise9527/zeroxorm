package zeroxorm

import (
	"io"
	"sync"

	"github.com/tal-tech/go-zero/core/syncx"
	"github.com/xormplus/xorm"
)

var connManager = syncx.NewResourceManager()

type pingedDB struct {
	*xorm.Engine
	once sync.Once
}

func getCachedConn(driverName, dataSource string) (*pingedDB, error) {
	val, err := connManager.GetResource(dataSource, func() (io.Closer, error) {
		engine, err := newDBConnection(driverName, dataSource)
		if err != nil {
			return nil, err
		}

		return &pingedDB{
			Engine: engine,
		}, nil
	})

	if err != nil {
		return nil, err
	}

	return val.(*pingedDB), nil
}

func getSqlConn(driverName, dataSource string) (*xorm.Engine, error) {
	pdb, err := getCachedConn(driverName, dataSource)
	if err != nil {
		return nil, err
	}

	pdb.once.Do(func() {
		err = pdb.Ping()
	})

	if err != nil {
		return nil, err
	}

	return pdb.Engine, nil
}

func newDBConnection(driverName, dataSource string) (*xorm.Engine, error) {
	return xorm.NewEngine(driverName, dataSource)
}
