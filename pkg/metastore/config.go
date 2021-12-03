package metastore

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
)

// TODO: this file has similar codes against master/config.go, we will refactor later

const (
	defaultPeerUrls            = "http://127.0.0.1:10341"
	defaultInitialClusterState = embed.ClusterStateFlagNew
)

var (
	// SampleConfigFile is sample config file of meta-store
	// later we can read it from dm/master/meta-store.toml
	// and assign it to SampleConfigFile while we build meta-store.
	SampleConfigFile string
)

// NewConfig creates a config for meta-store server.
func NewConfig() *Config {
	cfg := &Config{
		Etcd: &etcdutils.ConfigParams{},
	}
	cfg.flagSet = flag.NewFlagSet("meta-store", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.Addr, "addr", "", "meta store server API server and status addr")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${master-addr}")`)
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)

	fs.StringVar(&cfg.Etcd.Name, "name", "", "human-readable name for this meta-store member")
	fs.StringVar(&cfg.Etcd.DataDir, "data-dir", "/tmp/df/metastore/discovery", "data dir for service discovery meta store")
	fs.StringVar(&cfg.Etcd.InitialCluster, "initial-cluster", "", fmt.Sprintf("initial cluster configuration for bootstrapping, e.g. meta-store=%s", defaultPeerUrls))
	fs.StringVar(&cfg.Etcd.PeerUrls, "peer-urls", defaultPeerUrls, "URLs for peer traffic")
	fs.StringVar(&cfg.Etcd.AdvertisePeerUrls, "advertise-peer-urls", "", `advertise URLs for peer traffic (default "${peer-urls}")`)

	return cfg
}

// Config is the configuration for meta-store.
type Config struct {
	flagSet *flag.FlagSet

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	Addr          string `toml:"addr" json:"addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	ConfigFile string `toml:"config-file" json:"config-file"`

	// etcd relative config items
	Etcd *etcdutils.ConfigParams `toml:"etcd" json:"etcd"`

	printVersion      bool
	printSampleConfig bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal to json", zap.Reflect("master config", c), log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", log.ShortError(err))
	}

	return b.String(), nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Wrap(errors.ErrMasterConfigParseFlagSet, err)
	}

	if c.printSampleConfig {
		if strings.TrimSpace(SampleConfigFile) == "" {
			fmt.Println("sample config file of meta-store is empty")
		} else {
			rawConfig, err2 := base64.StdEncoding.DecodeString(SampleConfigFile)
			if err2 != nil {
				fmt.Println("base64 decode config error:", err2)
			} else {
				fmt.Println(string(rawConfig))
			}
		}
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.Wrap(errors.ErrMasterConfigParseFlagSet, err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.ErrMasterConfigInvalidFlag.GenWithStackByArgs(c.flagSet.Arg(0))
	}
	return c.adjust()
}

func (c *Config) adjust() (err error) {
	if c.Etcd.PeerUrls == "" {
		c.Etcd.PeerUrls = defaultPeerUrls
	}
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.Etcd.PeerUrls
	}
	if c.Etcd.AdvertisePeerUrls == "" {
		c.Etcd.AdvertisePeerUrls = c.Etcd.PeerUrls
	}
	if c.Etcd.InitialCluster == "" {
		items := strings.Split(c.Etcd.AdvertisePeerUrls, ",")
		for i, item := range items {
			items[i] = fmt.Sprintf("%s=%s", c.Etcd.Name, item)
		}
		c.Etcd.InitialCluster = strings.Join(items, ",")
	}

	if c.Etcd.InitialClusterState == "" {
		c.Etcd.InitialClusterState = defaultInitialClusterState
	}

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Wrap(errors.ErrMasterDecodeConfigFile, err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return errors.ErrMasterConfigUnknownItem.GenWithStackByArgs(strings.Join(undecodedItems, ","))
	}
	return nil
}
