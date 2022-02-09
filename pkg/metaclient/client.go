package metaclient

type ClientType int

const (
	TypeKvClient       ClientType = iota /* KV client style，like etcd/consul/TiKV/redis or even SQL backend*/
	TypeEtcdLikeClient                   /* Etcd client style, may need high-level api */
)

type KVClient interface {
	KV
	Close()
}
