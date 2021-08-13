package importer

const (
	MAX_IDLE_CONNS = 10
)

type ColumnType_T int32

const (
	CString ColumnType_T = iota
	CInt
)

type ColumnDDL struct {
	ColumnName string
	ColumnType ColumnType_T
	ColumnLen  int
}

type Config struct {
	User   string
	Passwd string
	Host   string
	Port   string
	Tables map[string][]string
}

func parse(configPath string) ([]*Config, []ColumnDDL) {

	return buildDefaultConfigs()
}

func buildDefaultConfigs() ([]*Config, []ColumnDDL) {
	tables := make(map[string][]string)
	tables["multi_test_db1"] = []string{"tbl1", "tbl2", "tbl3"}
	tables["multi_test_db2"] = []string{"tbl1", "tbl2", "tbl3"}
	tables["multi_test_db3"] = []string{"tbl1", "tbl2", "tbl3"}
	return []*Config{
			{
				User:   "root",
				Passwd: "",
				Host:   "127.0.0.1",
				Port:   "3306",
				Tables: tables,
			},
		}, []ColumnDDL{
			{
				ColumnName: "uid",
				ColumnType: CInt,
				ColumnLen:  10,
			},
			{
				ColumnName: "name",
				ColumnType: CString,
				ColumnLen:  20,
			},
			{
				ColumnName: "desc",
				ColumnType: CString,
				ColumnLen:  40,
			},
		}
}
