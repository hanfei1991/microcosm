package metaclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseOpUnion(t *testing.T) {
	t.Parallel()

	get := ResponseOp{
		Response: &ResponseOp_ResponseGet{
			ResponseGet: &GetResponse{
				Header: &ResponseHeader{
					ClusterId: "1111",
					Revision:  1111,
				},
			},
		},
	}
	require.IsType(t, &GetResponse{}, get.GetResponseGet())
	require.Nil(t, get.GetResponsePut())

	put := ResponseOp{
		Response: &ResponseOp_ResponsePut{
			ResponsePut: &PutResponse{
				Header: &ResponseHeader{
					ClusterId: "1111",
					Revision:  1111,
				},
			},
		},
	}
	require.IsType(t, &PutResponse{}, put.GetResponsePut())
	require.Nil(t, put.GetResponseDelete())

	delet := ResponseOp{
		Response: &ResponseOp_ResponseDelete{
			ResponseDelete: &DeleteResponse{
				Header: &ResponseHeader{
					ClusterId: "1111",
					Revision:  1111,
				},
			},
		},
	}
	require.IsType(t, &DeleteResponse{}, delet.GetResponseDelete())
	require.Nil(t, delet.GetResponseTxn())

	txn := ResponseOp{
		Response: &ResponseOp_ResponseTxn{
			ResponseTxn: &TxnResponse{
				Header: &ResponseHeader{
					ClusterId: "1111",
					Revision:  1111,
				},
			},
		},
	}
	require.IsType(t, &TxnResponse{}, txn.GetResponseTxn())
	require.Nil(t, txn.GetResponseGet())
}

func TestNestedTxnResponse(t *testing.T) {
	txn := ResponseOp{
		Response: &ResponseOp_ResponseTxn{
			ResponseTxn: &TxnResponse{
				Header: &ResponseHeader{
					ClusterId: "1111",
					Revision:  1111,
				},
				Responses: []ResponseOp{
					ResponseOp{
						Response: &ResponseOp_ResponseGet{
							ResponseGet: &GetResponse{
								Header: &ResponseHeader{
									ClusterId: "1111",
									Revision:  1111,
								},
							},
						},
					},
					ResponseOp{
						Response: &ResponseOp_ResponsePut{
							ResponsePut: &PutResponse{
								Header: &ResponseHeader{
									ClusterId: "1111",
									Revision:  1111,
								},
							},
						},
					},
					ResponseOp{
						Response: &ResponseOp_ResponseTxn{
							ResponseTxn: &TxnResponse{
								Header: &ResponseHeader{
									ClusterId: "1111",
									Revision:  1111,
								},
							},
						},
					},
				},
			},
		},
	}
	require.IsType(t, &TxnResponse{}, txn.GetResponseTxn())
	require.IsType(t, &GetResponse{}, txn.GetResponseTxn().Responses[0].GetResponseGet())
	require.IsType(t, &PutResponse{}, txn.GetResponseTxn().Responses[1].GetResponsePut())
	require.IsType(t, &TxnResponse{}, txn.GetResponseTxn().Responses[2].GetResponseTxn())
}
