//go:build integration

package integracion

import (
	"context"
	"testing"
	"time"

	"consultas-diarias/internal/scheduler"
)

// Idempotencia de punta a punta (spec §12.2): la misma franja ejecutada dos
// veces con Postgres y SQS reales deja en la cola los mensajes de una sola
// ejecución.
func TestEncolarFranjaDosVecesEncolaUnaVez(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	const slot = 7
	if _, err := e.Pool.Exec(ctx, `INSERT INTO registros (proveedor, endpoint, body, slot) VALUES
		('idem-a', '/x', '{}', $1), ('idem-a', '/y', '{}', $1), ('idem-b', '/z', '{}', $1)`, slot); err != nil {
		t.Fatal(err)
	}
	s := &scheduler.Scheduler{Fuente: e.Fuente, Cola: e.Cola}
	fecha := fechaDiaUnica()

	n1, err := s.EncolarFranja(ctx, fecha, slot)
	if err != nil || n1 != 3 {
		t.Fatalf("primera ejecución: %d encolados, %v", n1, err)
	}
	n2, err := s.EncolarFranja(ctx, fecha, slot)
	if err != nil || n2 != 0 {
		t.Fatalf("segunda ejecución: %d encolados, %v", n2, err)
	}

	esperar(t, 10*time.Second, func() bool { return e.mensajesVisibles(ctx, e.ColaURL) >= 3 })
	time.Sleep(time.Second)
	if n := e.mensajesVisibles(ctx, e.ColaURL); n != 3 {
		t.Fatalf("mensajes en la cola = %d, quiero 3", n)
	}
}
