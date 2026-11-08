package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

func armar(prog ...apiclient.Respuesta) (*Procesador, *cola.Fake, *resultados.Fake) {
	c := cola.NuevaFake()
	r := resultados.NuevaFake()
	p := &Procesador{
		Cola:       c,
		Resultados: r,
		API:        &apiclient.Fake{Programa: prog},
		Ahora:      func() time.Time { return time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC) },
		Rnd:        func() float64 { return 0.5 },
		Escala:     1,
	}
	return p, c, r
}

func entregar(t *testing.T, c *cola.Fake) dominio.MensajeRecibido {
	t.Helper()
	ctx := context.Background()
	_ = c.Enviar(ctx, []dominio.Mensaje{{RegistroID: 7, Proveedor: "acme", Endpoint: "/x", Body: []byte(`{}`), Fecha: "2026-11-08"}})
	got, _ := c.Recibir(ctx, 1)
	return got[0]
}

func TestProcesarExito(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 200, Body: []byte(`{"v":1}`)})
	m := entregar(t, c)
	if err := p.Procesar(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	res := r.Items["7:2026-11-08"]
	if res.Estado != dominio.EstadoOK || res.HTTPStatus != 200 || res.Intentos != 1 || string(res.Respuesta) != `{"v":1}` {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(c.Borrados) != 1 {
		t.Fatalf("el mensaje debía borrarse")
	}
}

func TestProcesarNoReintentable(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 400, Body: []byte(`{"error":"body invalido"}`)})
	m := entregar(t, c)
	_ = p.Procesar(context.Background(), m)
	res := r.Items["7:2026-11-08"]
	if res.Estado != dominio.EstadoFalloDefinitivo || res.HTTPStatus != 400 || res.Error == "" {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(c.Borrados) != 1 {
		t.Fatal("un 400 se borra sin reintentar")
	}
}

func TestProcesarReintentableReprograma(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 503})
	m := entregar(t, c)
	_ = p.Procesar(context.Background(), m)
	if len(r.Items) != 0 {
		t.Fatal("no debe guardar resultado en un reintento")
	}
	if len(c.Borrados) != 0 || c.Pendientes() != 1 {
		t.Fatal("el mensaje debía volver a la cola")
	}
	otra, _ := c.Recibir(context.Background(), 1)
	if otra[0].Intento != 2 {
		t.Fatalf("intento = %d", otra[0].Intento)
	}
}

func TestProcesarAgotado(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 503})
	m := entregar(t, c)
	m.Intento = dominio.IntentosMax
	_ = p.Procesar(context.Background(), m)
	res := r.Items["7:2026-11-08"]
	if res.Estado != dominio.EstadoAgotado || res.Intentos != 5 {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(c.Borrados) != 0 {
		t.Fatal("al agotar no se borra: SQS lo mueve a la DLQ por maxReceiveCount")
	}
}

func TestProcesarTimeoutEsReintentable(t *testing.T) {
	p, c, _ := armar(apiclient.Respuesta{Err: context.DeadlineExceeded})
	m := entregar(t, c)
	_ = p.Procesar(context.Background(), m)
	if c.Pendientes() != 1 {
		t.Fatal("timeout debía reprogramar")
	}
}

func TestProcesarResultadoYaExisteBorraIgual(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 200, Body: []byte(`{}`)})
	m := entregar(t, c)
	r.Items["7:2026-11-08"] = dominio.Resultado{Estado: dominio.EstadoOK}
	if err := p.Procesar(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(c.Borrados) != 1 {
		t.Fatal("si el resultado ya estaba, el mensaje es un duplicado y se borra")
	}
}

func TestProcesarFalloAlGuardarReprograma(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 200, Body: []byte(`{}`)})
	r.FallarN = 1
	m := entregar(t, c)
	err := p.Procesar(context.Background(), m)
	if err == nil || c.Pendientes() != 1 {
		t.Fatalf("fallo al guardar debe devolver error y reprogramar; err=%v", err)
	}
	if errors.Is(err, resultados.ErrYaExiste) {
		t.Fatal("no es ErrYaExiste")
	}
}
