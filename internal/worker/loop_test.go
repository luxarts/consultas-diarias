package worker

import (
	"context"
	"fmt"
	"sync"
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
	for time.Now().Before(limite) && (c.Pendientes() > 0 || c.EnVuelo() > 0) {
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

// apiContadora cuenta requests en vuelo por proveedor y guarda el máximo.
type apiContadora struct {
	mu      sync.Mutex
	enVuelo map[string]int
	maximo  map[string]int
	demora  time.Duration
}

func (a *apiContadora) Consultar(_ context.Context, proveedor, _ string, _ []byte) (int, []byte, error) {
	a.mu.Lock()
	a.enVuelo[proveedor]++
	if a.enVuelo[proveedor] > a.maximo[proveedor] {
		a.maximo[proveedor] = a.enVuelo[proveedor]
	}
	a.mu.Unlock()
	time.Sleep(a.demora)
	a.mu.Lock()
	a.enVuelo[proveedor]--
	a.mu.Unlock()
	return 200, []byte(`{}`), nil
}

// El fake de cola entrega, como SQS, varios mensajes del mismo grupo en un
// receive con max > 1. Con varias goroutines por worker, la única forma de
// no tener dos requests simultáneos al mismo proveedor es pedir de a uno.
func TestCorrerNuncaDosEnVueloPorProveedor(t *testing.T) {
	c := cola.NuevaFake()
	r := resultados.NuevaFake()
	var msgs []dominio.Mensaje
	for i := int64(1); i <= 24; i++ {
		prov := "unico"
		if i > 16 {
			prov = fmt.Sprintf("otro-%d", i%2)
		}
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: prov, Fecha: "2026-11-08", Body: []byte(`{}`)})
	}
	_ = c.Enviar(context.Background(), msgs)
	api := &apiContadora{enVuelo: map[string]int{}, maximo: map[string]int{}, demora: 5 * time.Millisecond}
	p := &Procesador{Cola: c, Resultados: r, API: api, Ahora: time.Now, Rnd: func() float64 { return 0.5 }, Escala: 1}

	ctx, cancel := context.WithCancel(context.Background())
	hecho := make(chan struct{})
	go func() { Correr(ctx, p, 4); close(hecho) }()

	limite := time.Now().Add(10 * time.Second)
	for time.Now().Before(limite) && (c.Pendientes() > 0 || c.EnVuelo() > 0) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-hecho

	if len(c.Borrados) != 24 {
		t.Fatalf("borrados %d de 24", len(c.Borrados))
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	for prov, max := range api.maximo {
		if max > 1 {
			t.Errorf("proveedor %s tuvo %d requests simultáneos", prov, max)
		}
	}
}

// apiBloqueante no responde hasta que se cierra liberar.
type apiBloqueante struct {
	entro   chan struct{}
	liberar chan struct{}
}

func (a *apiBloqueante) Consultar(_ context.Context, _, _ string, _ []byte) (int, []byte, error) {
	a.entro <- struct{}{}
	<-a.liberar
	return 200, []byte(`{}`), nil
}

// Al cancelar el contexto, Correr deja de recibir pero termina el mensaje en
// vuelo: lo guarda y lo borra antes de volver.
func TestCorrerTerminaElMensajeEnVueloAlApagar(t *testing.T) {
	c := cola.NuevaFake()
	r := resultados.NuevaFake()
	_ = c.Enviar(context.Background(), []dominio.Mensaje{
		{RegistroID: 1, Proveedor: "a", Fecha: "2026-11-08", Body: []byte(`{}`)},
		{RegistroID: 2, Proveedor: "a", Fecha: "2026-11-08", Body: []byte(`{}`)},
	})
	api := &apiBloqueante{entro: make(chan struct{}, 2), liberar: make(chan struct{})}
	p := &Procesador{Cola: c, Resultados: r, API: api, Ahora: time.Now, Rnd: func() float64 { return 0.5 }, Escala: 1}

	ctx, cancel := context.WithCancel(context.Background())
	hecho := make(chan struct{})
	go func() { Correr(ctx, p, 2); close(hecho) }()

	<-api.entro
	cancel()
	select {
	case <-hecho:
		t.Fatal("Correr volvió con un mensaje en vuelo")
	case <-time.After(50 * time.Millisecond):
	}
	close(api.liberar)
	select {
	case <-hecho:
	case <-time.After(3 * time.Second):
		t.Fatal("Correr no terminó tras liberar el mensaje en vuelo")
	}
	if len(c.Borrados) != 1 || c.Borrados[0] != 1 {
		t.Fatalf("debía borrar sólo el 1: %v", c.Borrados)
	}
	if c.Pendientes() != 1 {
		t.Fatal("el 2 no debía recibirse después de cancelar")
	}
}
