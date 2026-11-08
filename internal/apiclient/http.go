package apiclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTP llama a la API interna en {base}/{proveedor}{endpoint}.
type HTTP struct {
	base    string
	cliente *http.Client
}

func NuevoHTTP(base string, timeout time.Duration) *HTTP {
	return &HTTP{
		base:    strings.TrimRight(base, "/"),
		cliente: &http.Client{Timeout: timeout},
	}
}

func (h *HTTP) Consultar(ctx context.Context, proveedor, endpoint string, body []byte) (int, []byte, error) {
	url := h.base + "/" + proveedor + "/" + strings.TrimLeft(endpoint, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.cliente.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}
