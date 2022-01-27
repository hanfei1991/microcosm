package metaclient

import "context"

// ResponseHeader is common response header
type ResponseHeader struct {
	// ClusterId is the ID of the cluster which sent the response.
	// Framework will generate uuid to generate every newcoming metastore
	ClusterId string
	// Revision is the key-value store revision when the request was applied.
	Revision int64
}

// Put Response
type PutResponse struct {
	Header *ResponseHeader
}

// Get Response
type GetResponse struct {
	Header *ResponseHeader
	// kvs is the list of key-value pairs matched by the range request.
	// kvs is empty when count is requested.
	Kvs []*KeyValue
}

// Delete Response
type DeleteResponse struct {
	Header *ResponseHeader
}

// Txn Response
type TxnResponse struct {
	Header *ResponseHeader
	// Responses is a list of responses corresponding to the results from applying
	// success if succeeded is true or failure if succeeded is false.
	Responses []ResponseOp
}

type ResponseOp struct {
	// response is a union of response types returned by a transaction.
	//
	// Types that are valid to be assigned to Response:
	//	*ResponseOp_ResponseRange
	//	*ResponseOp_ResponsePut
	//	*ResponseOp_ResponseDeleteRange
	//	*ResponseOp_ResponseTxn
	Response isResponseOp_Response
}

// Using interface to make union
type isResponseOp_Response interface {
	isResponseOp_Response()
}

type ResponseOp_ResponseGet struct {
	ResponseGet *GetResponse
}
type ResponseOp_ResponsePut struct {
	ResponsePut *PutResponse
}
type ResponseOp_ResponseDelete struct {
	ResponseDelete *DeleteResponse
}
type ResponseOp_ResponseTxn struct {
	ResponseTxn *TxnResponse
}

func (*ResponseOp_ResponseGet) isResponseOp_Response()    {}
func (*ResponseOp_ResponsePut) isResponseOp_Response()    {}
func (*ResponseOp_ResponseDelete) isResponseOp_Response() {}
func (*ResponseOp_ResponseTxn) isResponseOp_Response()    {}

func (m *ResponseOp) GetResponse() isResponseOp_Response {
	if m != nil {
		return m.Response
	}
	return nil
}

func (m *ResponseOp) GetResponseGet() *GetResponse {
	if x, ok := m.GetResponse().(*ResponseOp_ResponseGet); ok {
		return x.ResponseGet
	}
	return nil
}

func (m *ResponseOp) GetResponsePut() *PutResponse {
	if x, ok := m.GetResponse().(*ResponseOp_ResponsePut); ok {
		return x.ResponsePut
	}
	return nil
}

func (m *ResponseOp) GetResponseDelete() *DeleteResponse {
	if x, ok := m.GetResponse().(*ResponseOp_ResponseDelete); ok {
		return x.ResponseDelete
	}
	return nil
}

func (m *ResponseOp) GetResponseTxn() *TxnResponse {
	if x, ok := m.GetResponse().(*ResponseOp_ResponseTxn); ok {
		return x.ResponseTxn
	}
	return nil
}

type KeyValue struct {
	// Key is the key in bytes. An empty key is not allowed.
	Key []byte
	// Value is the value held by the key, in bytes.
	Value []byte
	// Remaining TTL for the key
	TTL uint64
	// create_revision is the revision of last creation on this key.
	CreateRevision int64
	// mod_revision is the revision of last modification on this key.
	ModRevision int64
}

type Txn interface {
	// Cache Ops in the Txn
	Do(ops ...Op) Txn

	// Commit tries to commit the transaction.
	// Any Op error will cause entire txn rollback and return error
	Commit(ctx context.Context) (*TxnResponse, error)
}

type KV interface {
	// Put puts a key-value pair into metastore.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error)

	// Get retrieves keys.
	// By default, Get will return the value for "key", if any.
	// When passed WithRange(end), Get will return the keys in the range [key, end).
	// When passed WithFromKey(), Get returns keys greater than or equal to key.
	// When passed WithLimit(limit), the number of returned keys is bounded by limit.
	// When passed WithSort(), the keys will be sorted.
	Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error)

	// Txn creates a transaction.
	Txn(ctx context.Context) Txn
}

// [TODO] Wrap a high-level txn interface to support CompareAndDo txn
