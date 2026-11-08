//go:build integration

package integracion

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/worker"
)

func lanzarWorker(ctx context.Context, e *entorno, escala float64) {
	p := &worker.Procesador{Cola: e.Cola, Resultados: e.Resultados,
		API: apiclient.NuevoHTTP(e.APIBase, dominio.TimeoutHTTP), Ahora: time.Now, Rnd: rand.Float64, Escala: escala}
	go worker.Correr(ctx, p, 2)
}

func enviarUno(t *testing.T, e *entorno, id int64, proveedor, secuencia, fecha string) {
	t.Helper()
	body := `{"_forzar_secuencia":"` + secuencia + `"}`
	if err := e.Cola.Enviar(context.Background(), []dominio.Mensaje{
		{RegistroID: id, Proveedor: proveedor, Endpoint: "/c", Body: []byte(body), Fecha: fecha},
	}); err != nil {
		t.Fatal(err)
	}
}

// Escala 600: el backoff de 30 s pasa a 50 ms, el de 2 min a 200 ms.
func TestReintentoExitoso(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fecha := fechaUnica()
	enviarUno(t, e, 101, "reint-ok-"+fecha, "503,500,200", fecha)
	lanzarWorker(ctx, e, 600)
	esperar(t, 30*time.Second, func() bool {
		_, ok, _ := e.Resultados.Leer(ctx, 101, fecha)
		return ok
	})
	r, _, _ := e.Resultados.Leer(ctx, 101, fecha)
	if r.Estado != dominio.EstadoOK || r.Intentos != 3 || r.HTTPStatus != 200 {
		t.Fatalf("resultado: %+v", r)
	}
}

func TestNoReintentableEnUnaPasada(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fecha := fechaUnica()
	enviarUno(t, e, 102, "fallo-400-"+fecha, "400", fecha)
	lanzarWorker(ctx, e, 600)
	esperar(t, 30*time.Second, func() bool {
		_, ok, _ := e.Resultados.Leer(ctx, 102, fecha)
		return ok
	})
	r, _, _ := e.Resultados.Leer(ctx, 102, fecha)
	if r.Estado != dominio.EstadoFalloDefinitivo || r.Intentos != 1 || r.HTTPStatus != 400 {
		t.Fatalf("resultado: %+v", r)
	}
}

func TestAgotadoTerminaEnDLQ(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fecha := fechaUnica()
	// El conteo inicial hace la prueba repetible aunque la DLQ no esté vacía.
	inicial := e.mensajesVisibles(ctx, e.DLQURL)
	if inicial < 0 {
		t.Fatal("no se pudo leer la DLQ")
	}
	enviarUno(t, e, 103, "siempre-503-"+fecha, "503,503,503,503,503,503", fecha)
	lanzarWorker(ctx, e, 600)
	esperar(t, 60*time.Second, func() bool {
		r, ok, _ := e.Resultados.Leer(ctx, 103, fecha)
		return ok && r.Estado == dominio.EstadoAgotado
	})
	r, _, _ := e.Resultados.Leer(ctx, 103, fecha)
	if r.Intentos != dominio.IntentosMax {
		t.Fatalf("intentos = %d", r.Intentos)
	}
	// Tras el quinto intento el worker pone la visibilidad en 0: en la
	// siguiente recepción SQS ve que superó maxReceiveCount y lo mueve a la
	// DLQ, sin esperar los 60 s del visibility timeout.
	esperar(t, 60*time.Second, func() bool {
		return e.mensajesVisibles(ctx, e.DLQURL) >= inicial+1
	})
}
