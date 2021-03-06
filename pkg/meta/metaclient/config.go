package metaclient

import (
	"fmt"
	"strings"
)

// defines const variables used in metastore client
const (
	FrameMetaID       = "root"
	DefaultUserMetaID = "default"

	DefaultUserMetaEndpoints = "127.0.0.1:12479"
)

// AuthConfParams is basic password authentication configurations
type AuthConfParams struct {
	User   string `toml:"user" json:"user"`
	Passwd string `toml:"passwd" json:"passwd"`
}

// StoreConfigParams is metastore connection configurations
type StoreConfigParams struct {
	// storeID is the unique readable identifier for a store
	StoreID string `toml:"store-id" json:"store-id"`
	// TODO: replace the slice when we migrate to db
	Endpoints []string       `toml:"endpoints" json:"endpoints"`
	Auth      AuthConfParams `toml:"auth" json:"auth"`
}

// SetEndpoints sets endpoints to StoreConfigParams
func (s *StoreConfigParams) SetEndpoints(endpoints string) {
	if endpoints != "" {
		s.Endpoints = strings.Split(endpoints, ",")
	}
}

// GenerateDsn generates dsn string from store config parameters
// dsn format: [username[:password]@][protocol[(address)]]
func (s *StoreConfigParams) GenerateDsn() string {
	if len(s.Endpoints) == 0 {
		return ""
	}

	return fmt.Sprintf("%s:%s@tcp(%s)", s.Auth.User, s.Auth.Passwd, s.Endpoints[0])
}
