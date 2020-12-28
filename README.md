# zeroxorm
利用xorm来实现go-zero中的sqlx.SqlConn接口,把xorm集成到go-zero中.

实现逻辑是参考go-zero中的sqlx.commonSqlConn来的.

## oracle
``` go
// 创建sqlx.SqlConn对象,针对oracle.
conn := zeroxorm.NewZeroOracle(c.DataSource)

// 使用方式与sqlx.NewMySql(c.DataSource)创建出来的对象行为完全一致.
```

## mysql
``` go
// 创建sqlx.SqlConn对象,针对mysql.
conn := zeroxorm.NewZeroMysql(c.DataSource)

// 使用方式与sqlx.NewMySql(c.DataSource)创建出来的对象行为完全一致.
```