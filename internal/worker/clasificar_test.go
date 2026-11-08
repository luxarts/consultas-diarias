package worker

import (
	"context"
	"errors"
	"net"
	"testing"

	"consultas-diarias/internal/dominio"
)

func TestClasificar(t *testing.T) {
	casos := []struct {
		nombre string
		status int
		body   string
		err    error
		quiero dominio.Clasificacion
	}{
		{"200 con json", 200, `{"ok":true}`, nil, dominio.Exito},
		{"201", 201, `{}`, nil, dominio.Exito},
		{"200 con body invalido", 200, `no es json`, nil, dominio.NoReintentable},
		{"400", 400, ``, nil, dominio.NoReintentable},
		{"401", 401, ``, nil, dominio.NoReintentable},
		{"403", 403, ``, nil, dominio.NoReintentable},
		{"404", 404, ``, nil, dominio.NoReintentable},
		{"405", 405, ``, nil, dominio.NoReintentable},
		{"422", 422, ``, nil, dominio.NoReintentable},
		{"418 otro 4xx", 418, ``, nil, dominio.NoReintentable},
		{"429", 429, ``, nil, dominio.Reintentable},
		{"500", 500, ``, nil, dominio.Reintentable},
		{"502", 502, ``, nil, dominio.Reintentable},
		{"503", 503, ``, nil, dominio.Reintentable},
		{"504", 504, ``, nil, dominio.Reintentable},
		{"timeout", 0, ``, context.DeadlineExceeded, dominio.Reintentable},
		{"error de red", 0, ``, &net.OpError{Op: "dial", Err: errors.New("refused")}, dominio.Reintentable},
		{"error generico", 0, ``, errors.New("algo"), dominio.Reintentable},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			got := Clasificar(c.status, []byte(c.body), c.err)
			if got != c.quiero {
				t.Fatalf("Clasificar(%d, %q, %v) = %s, quiero %s", c.status, c.body, c.err, got, c.quiero)
			}
		})
	}
}
