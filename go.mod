module github.com/shawnfeng/dbrouter

replace (
	code.google.com/p/go-uuid => github.com/shawnfeng/googleuuid v1.0.0
	code.google.com/p/goprotobuf => github.com/shawnfeng/googlpb v1.0.0
)

require (
	github.com/bitly/go-simplejson v0.4.4-0.20140701141959-3378bdcb5ceb
	// nn
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/go-sql-driver/mysql v1.0.1-0.20160411075031-7ebe0a500653
	github.com/jmoiron/sqlx v0.0.0-20170430194603-d9bd385d68c0
	github.com/lib/pq v0.0.0-20170603225454-8837942c3e09
	// nn
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/shawnfeng/sutil v1.0.0
	gopkg.in/mgo.v2 v2.0.0-20141107142503-e2e914857713

)
