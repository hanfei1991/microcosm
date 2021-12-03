package benchmark

import (
	"encoding/json"
	"flag"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("microcosm", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	return cfg
}

type Config struct {
	flagSet *flag.FlagSet `json:"-"`

	// FlowID is a unique identifier of submitted job
	FlowID   string   `toml:"flow-id" json:"flow-id"`
	TableNum int32    `toml:"table-num" json:"table-num"`
	Servers  []string `toml:"servers"   json:"servers"`

	RecordCnt    int32 `toml:"rcd-cnt" json:"rcd-cnt"`
	DDLFrequency int32 `toml:"ddl-freq" json:"ddl-freq"`

	configFile string `json:"-"`
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func configFromJSON(j string) (*Config, error) {
	c := NewConfig()
	err := json.Unmarshal([]byte(j), c)
	return c, err
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		return errors.New("please designate a config file")
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return nil
}
