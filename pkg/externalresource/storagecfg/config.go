package storagecfg

type Config struct {
	Local *LocalFileConfig `json:"local" toml:"local"`
}

type LocalFileConfig struct {
	BaseDir string `json:"base_dir" toml:"base-dir"`
}
