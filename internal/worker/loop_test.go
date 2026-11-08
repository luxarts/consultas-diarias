package worker

import (
	"context"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

func TestCorrerProcesaTodoYTermina(t *testing.T) {
	c := cola.NuevaFake()
	r := resultados.NuevaFake()
	var msgs []dominio.Mensaje
	for i := int64(1); i <= 30; i++ {
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: "p" + string(rune('a'+i%5)), Fecha: "2026-11-08", Body: []byte(`{}`)})
	}
	_ = c.Enviar(context.Background(), msgs)
	p := &Procesador{Cola: c, Resultados: r, API: &apiclient.Fake{Programa: []apiclient.Respuesta{{Status: 200, Body: []byte(`{}`)}}},
		Ahora: time.Now, Rnd: func() float64 { return 0.5 }, Escala: 1}

	ctx, cancel := context.WithCancel(context.Background())
	hecho := make(chan struct{})
	go func() { Correr(ctx, p, 4); close(hecho) }()

	limite := time.Now().Add(5 * time.Second)
	for time.Now().Before(limite) && c.Pendientes() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-hecho:
	case <-time.After(3 * time.Second):
		t.Fatal("Correr no terminó tras cancelar el contexto")
	}
	if len(r.Items) != 30 {
		t.Fatalf("procesados %d de 30", len(r.Items))
	}
}
