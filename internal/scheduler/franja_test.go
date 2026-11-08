package scheduler

import (
	"context"
	"testing"

	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/fuente"
)

func regs() []dominio.Registro {
	return []dominio.Registro{
		{ID: 1, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Slot: 5},
		{ID: 2, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Slot: 5},
		{ID: 3, Proveedor: "b", Endpoint: "/y", Body: []byte(`{}`), Slot: 5},
		{ID: 4, Proveedor: "c", Endpoint: "/z", Body: []byte(`{}`), Slot: 6},
	}
}

func TestEncolarFranjaPublicaLosDelSlot(t *testing.T) {
	f := fuente.NuevaFake(regs())
	c := cola.NuevaFake()
	s := &Scheduler{Fuente: f, Cola: c}
	n, err := s.EncolarFranja(context.Background(), "2026-11-08", 5)
	if err != nil || n != 3 || c.Enviados != 3 {
		t.Fatalf("n=%d enviados=%d err=%v", n, c.Enviados, err)
	}
	if f.Franjas["2026-11-08:5"] != 3 {
		t.Fatalf("la franja debía cerrarse con 3: %v", f.Franjas)
	}
	got, _ := c.Recibir(context.Background(), 10)
	if got[0].Fecha != "2026-11-08" || got[0].Endpoint != "/x" {
		t.Fatalf("mensaje mal armado: %+v", got[0])
	}
}

func TestEncolarFranjaEsIdempotente(t *testing.T) {
	f := fuente.NuevaFake(regs())
	c := cola.NuevaFake()
	s := &Scheduler{Fuente: f, Cola: c}
	_, _ = s.EncolarFranja(context.Background(), "2026-11-08", 5)
	n, err := s.EncolarFranja(context.Background(), "2026-11-08", 5)
	if err != nil || n != 0 || c.Enviados != 3 {
		t.Fatalf("segunda ejecución no debía encolar: n=%d enviados=%d err=%v", n, c.Enviados, err)
	}
}

func TestEncolarFranjaSaltaProveedoresAtrasados(t *testing.T) {
	f := fuente.NuevaFake(regs())
	f.Atrasados = []string{"a"}
	c := cola.NuevaFake()
	s := &Scheduler{Fuente: f, Cola: c}
	n, _ := s.EncolarFranja(context.Background(), "2026-11-08", 5)
	if n != 1 || c.Enviados != 1 {
		t.Fatalf("sólo 'b' debía encolarse: n=%d", n)
	}
	got, _ := c.Recibir(context.Background(), 10)
	if got[0].Proveedor != "b" {
		t.Fatalf("got %+v", got[0])
	}
}
