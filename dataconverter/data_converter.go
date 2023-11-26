package dataconverter

import (
	"net/http"
	"strings"

	"go.temporal.io/sdk/converter"
)

var (
	dataConverter = converter.GetDefaultDataConverter()
)

func DefaultDataConverter() converter.DataConverter {
	return converter.GetDefaultDataConverter()
}

func CustomDataConverter() converter.DataConverter {
	return dataConverter
}

func SetRemoteEndpoint(endpoint string, namespace string, auth string) {
	endpoint = strings.ReplaceAll(endpoint, "{namespace}", namespace)

	dataConverter = converter.NewRemoteDataConverter(
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
