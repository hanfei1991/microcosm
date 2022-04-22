package metaclient

import (
	"fmt"
	"strings"
)

const (
	FrameMetaID       = "root"
	DefaultUserMetaID = "default"

	DefaultUserMetaEndpoints = "127.0.0.1:12479"
)

type StoreConfigParams struct {
	// storeID is the unique readable identifier for a store
	StoreID   string   `toml:"store-id" json:"store-id"`
	Endpoints []string `toml:"endpoints" json:"endpoints"`
	User      string   `toml:"user" json:"user"`
	Password  string   `toml:"password" json:"password"`
}

func (s *StoreConfigParams) SetEndpoints(endpoints string) {
	if endpoints != "" {
		s.Endpoints = strings.Split(endpoints, ",")
	}
}

// dsn format: [username[:password]@][protocol[(address)]]
func (s *StoreConfigParams) GenerateDsn() string {
	if len(s.Endpoints) == 0 {
		return ""
	}

	return fmt.Sprintf("%s:%s@tcp(%s)", s.User, s.Password, s.Endpoints[0])
}
