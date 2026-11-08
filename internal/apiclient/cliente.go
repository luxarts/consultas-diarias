// Package apiclient llama a la API interna.
package apiclient

import (
	"context"
	"sync"
)

type ClienteAPI interface {
	// Consultar devuelve el status HTTP y el body. err distinto de nil sólo
	// para fallas de transporte (timeout, conexión); un 500 no es error.
	Consultar(ctx context.Context, proveedor, endpoint string, body []byte) (int, []byte, error)
}

// Respuesta es un resultado programado del Fake.
type Respuesta struct {
	Status int
	Body   []byte
	Err    error
}

// Fake devuelve respuestas en orden; repite la última cuando se agotan.
type Fake struct {
	mu       sync.Mutex
	Programa []Respuesta
	Llamadas int
}

func (f *Fake) Consultar(_ context.Context, _, _ string, _ []byte) (int, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.Llamadas
	if i >= len(f.Programa) {
		i = len(f.Programa) - 1
	}
	f.Llamadas++
	r := f.Programa[i]
	return r.Status, r.Body, r.Err
}
