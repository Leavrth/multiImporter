package importer

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
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
	tableDDL []ColumnDDL
}

func NewImporter(path string) (*Importer, error) {
	configs, tableDDL := parse(path)
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
		tableDDL,
	}, nil
}

func (i *Importer) Close() {
	for _, tableDB := range i.tableDBs {
		tableDB.db.Close()
	}
}

func generateInsertQueryTemplate(tableDDL []ColumnDDL) string {
	var strBuilder strings.Builder
	var argBuilder strings.Builder
	for i, columnDDL := range tableDDL {
		var str1 string
		if i != len(tableDDL) {
			str1 = fmt.Sprintf("`%s`, ", columnDDL.ColumnName)
			argBuilder.WriteString("?, ")
		} else {
			str1 = fmt.Sprintf("`%s`", columnDDL.ColumnName)
			argBuilder.WriteString("?")
		}
		strBuilder.WriteString(str1)
	}
	return fmt.Sprintf("INSERT INTO %%s.%%s (%s) VALUES (%s)", strBuilder.String(), argBuilder.String())

}

func randString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func randInt(len int) int {
	return rand.Intn(1 << len)
}

func (impt *Importer) Start(ctx context.Context) error {
	ctxx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	argsCh := make(chan []interface{}, 1024)
	errCh := make(chan error)
	queryTmpl := generateInsertQueryTemplate(impt.tableDDL)
	for _, tableDB := range impt.tableDBs {
		for schema, tables := range tableDB.tables {
			for _, table := range tables {
				wg.Add(1)
				go func(ctx context.Context, db *sql.DB, schema, table string, argsCh chan []interface{}, errCh chan error) {
					defer wg.Done()
					tx, err := db.Begin()
					if err != nil {
						select {
						case <-ctxx.Done():
						case errCh <- err:
						}
						return
					}

					stmt, err := tx.Prepare(fmt.Sprintf(queryTmpl, schema, table))
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
						case args, ok := <-argsCh:
							if !ok && args == nil {
								fmt.Println("Finish, break!")
								tx.Commit()
								return
							}
							_, err := stmt.Exec(args)
							if err != nil {
								select {
								case <-ctxx.Done():
								case errCh <- err:
								}
								return
							}
						}
					}

				}(ctxx, tableDB.db, schema, table, argsCh, errCh)
			}
		}
	}

	rand.Seed(time.Now().UnixNano())

	// uid int, name varchar(20), desc varchar(40)
	for i := 0; i < 1000000; i++ {
		args := make([]interface{}, len(impt.tableDDL))
		for j, columnDDL := range impt.tableDDL {
			switch columnDDL.ColumnType {
			case CString:
				args[j] = randString(columnDDL.ColumnLen)
			case CInt:
				args[j] = randInt(columnDDL.ColumnLen)
			}
		}
		select {
		case argsCh <- args:

		case err := <-errCh:
			return err
		}
	}
	close(argsCh)
	wg.Wait()
	return nil
}
