package metaclient

import (
	"time"
)

type Config struct {
	// Endpoints is a list of URLs.
	Endpoints []string `json:"endpoints"`
	Auth      AuthConf
	Dial      DialConf
	Log       LogConf
}

type AuthConf struct {
	// [TODO] TLS holds the client secure credentials, if any.

	// Username is a user name for authentication.
	Username string `json:"username"`

	// Password is a password for authentication.
	Password string `json:"password"`
}

type DialConf struct {
	// DialTimeout is the timeout for failing to establish a connection.
	DialTimeout time.Duration `json:"dial-timeout"`

	// DialKeepAliveTime is the time after which client pings the server to see if
	// transport is alive.
	DialKeepAliveTime time.Duration `json:"dial-keep-alive-time"`

	// DialKeepAliveTimeout is the time that the client waits for a response for the
	// keep-alive probe. If the response is not received in this time, the connection is closed.
	DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"`

	// MaxSendMsgSize is the client-side request send limit in bytes.
	// If 0, it defaults to 2.0 MiB (2 * 1024 * 1024).
	// Make sure that "MaxSendMsgSize" < server-side default send/recv limit.
	// ("--max-request-bytes" flag to etcd or "embed.Config.MaxRequestBytes").
	MaxSendMsgSize int

	// MaxRecvMsgSize is the client-side response receive limit.
	// If 0, it defaults to "math.MaxInt32", because range response can
	// easily exceed request send limits.
	// Make sure that "MaxRecvMsgSize" >= server-side default send/recv limit.
	// ("--max-request-bytes" flag to etcd or "embed.Config.MaxRequestBytes").
	MaxRecvMsgSize int

	// DialOptions is a list of dial options for the different client
	// Set this DialOptions if client need some specified initial options
	DialOptions []interface{}
}

type LogConf struct {
	File  string
	level int
}

func (cf *Config) Clone() *Config {
	newConf := *cf
	return &newConf
}
