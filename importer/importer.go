package importer

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type tableDB struct {
	tables map[string][]string
	db     *sql.DB
}

type Importer struct {
	tableDBs []*tableDB
}

func NewImporter(path string) (*Importer, error) {
	configs := parse(path)
	tableDBs := make([]*tableDB, 0, len(configs))
	for _, config := range configs {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)", config.User, config.Passwd, config.Host, config.Port))
		if err != nil {
			return nil, err
		}
		db.SetMaxIdleConns(MAX_IDLE_CONNS)
		if err := db.Ping(); err != nil {
			fmt.Println("open database fail")
			return nil, err
		}
		tableDBs = append(tableDBs, &tableDB{
			tables: config.Tables,
			db:     db,
		})
	}
	return &Importer{
		tableDBs,
	}, nil
}

func (i *Importer) Close() {
	for _, tableDB := range i.tableDBs {
		tableDB.db.Close()
	}
}

func randString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

type ARGs struct {
	uid  int
	name string
	desc string
}

func (i *Importer) Start(ctx context.Context) error {
	ctxx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	sqlCh := make(chan *ARGs, 1024)
	errCh := make(chan error)
	for _, tableDB := range i.tableDBs {
		for schema, tables := range tableDB.tables {
			for _, table := range tables {
				wg.Add(1)
				go func(ctx context.Context, db *sql.DB, schema, table string, sqlCh chan *ARGs, errCh chan error) {
					defer wg.Done()
					tx, err := db.Begin()
					if err != nil {
						select {
						case <-ctxx.Done():
						case errCh <- err:
						}
						return
					}

					stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s.%s (`uid`, `name`, `desc`) VALUES (?, ?, ?)", schema, table))
					if err != nil {
						select {
						case <-ctxx.Done():
						case errCh <- err:
						}
						return
					}
					for {
						select {
						case <-ctxx.Done():
							return
						case sql, ok := <-sqlCh:
							if !ok && sql == nil {
								fmt.Println("Finish, break!")
								tx.Commit()
								return
							}
							_, err := stmt.Exec(sql.uid, sql.name, sql.desc)
							if err != nil {
								select {
								case <-ctxx.Done():
								case errCh <- err:
								}
								return
							}
						}
					}

				}(ctxx, tableDB.db, schema, table, sqlCh, errCh)
			}
		}
	}

	rand.Seed(time.Now().UnixNano())

	// uid int, name varchar(20), desc varchar(40)
	for uid := 0; uid < 1000000; uid++ {
		name := randString(19)
		desc := randString(39)
		args := &ARGs{
			uid,
			name,
			desc,
		}
		fmt.Printf("[uid = %d][name = %s][desc = %s]\n", uid, name, desc)
		select {
		case sqlCh <- args:

		case err := <-errCh:
			return err
		}
	}
	close(sqlCh)
	wg.Wait()
	return nil
}
