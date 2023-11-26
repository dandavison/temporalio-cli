package dataconverter

import (
	"net/http"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type IdentityPayloadCodec struct{}

func (c IdentityPayloadCodec) Encode(payload []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payload, nil
}

func (c IdentityPayloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	return payloads, nil
}

var (
	dataConverter                        = converter.GetDefaultDataConverter()
	payloadCodec  converter.PayloadCodec = IdentityPayloadCodec{}
)

func DefaultDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}

func CustomDataConverter() converter.DataConverter {
	return dataConverter
}

func CustomPayloadCodec() converter.PayloadCodec {
	return payloadCodec
}

func SetRemoteEndpoint(endpoint string, namespace string, auth string) {
	endpoint = strings.ReplaceAll(endpoint, "{namespace}", namespace)

	payloadCodec = converter.NewRemotePayloadCodec(
		converter.GetDefaultDataConverter(),
		converter.RemoteDataConverterOptions{
			Endpoint: endpoint,
			ModifyRequest: func(req *http.Request) error {
				req.Header.Set("X-Namespace", namespace)
				if auth != "" {
					req.Header.Set("Authorization", auth)
				}

				return nil
			},
		},
	)
}
