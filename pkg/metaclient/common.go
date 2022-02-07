package metaclient

// ResponseHeader is common response header
type ResponseHeader struct {
	// ClusterId is the ID of the cluster which sent the response.
	// Framework will generate uuid for every newcoming metastore
	ClusterId string
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

type OpResponse struct {
	put *PutResponse
	get *GetResponse
	del *DeleteResponse
	txn *TxnResponse
}

func (op OpResponse) Put() *PutResponse    { return op.put }
func (op OpResponse) Get() *GetResponse    { return op.get }
func (op OpResponse) Del() *DeleteResponse { return op.del }
func (op OpResponse) Txn() *TxnResponse    { return op.txn }

func (resp *PutResponse) OpResponse() OpResponse {
	return OpResponse{put: resp}
}
func (resp *GetResponse) OpResponse() OpResponse {
	return OpResponse{get: resp}
}
func (resp *DeleteResponse) OpResponse() OpResponse {
	return OpResponse{del: resp}
}
func (resp *TxnResponse) OpResponse() OpResponse {
	return OpResponse{txn: resp}
}

type KeyValue struct {
	// Key is the key in bytes. An empty key is not allowed.
	Key []byte
	// Value is the value held by the key, in bytes.
	Value []byte
	// Remaining TTL for the key
	TTL int64
	// Revision is the unique identifier for the key,
	// user can always expect an increasing revision after each operation.
	// So `delete + create` will not make revision fallback, which will avoid ABA problem.
	Revision int64
}
