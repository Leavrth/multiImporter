package importer

const (
	MAX_IDLE_CONNS = 10
)

type Config struct {
	User   string
	Passwd string
	Host   string
	Port   string
	Tables map[string][]string
}

func parse(configPath string) []*Config {

	return []*Config{}
}
