//go:build integration

package integracion

import (
	"context"
	"errors"
	"testing"
	"time"

	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

func TestSQSIdaYVuelta(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	msgs := []dominio.Mensaje{
		{RegistroID: 1, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Fecha: "2026-11-08"},
		{RegistroID: 2, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Fecha: "2026-11-08"},
	}
	if err := e.Cola.Enviar(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	got, err := e.Cola.Recibir(ctx, 10)
	if err != nil || len(got) != 1 || got[0].RegistroID != 1 || got[0].Intento != 1 {
		t.Fatalf("FIFO debía entregar sólo el primero del grupo: %+v, %v", got, err)
	}
	if err := e.Cola.CambiarVisibilidad(ctx, got[0].Recibo, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	otra, _ := e.Cola.Recibir(ctx, 10)
	if len(otra) != 1 || otra[0].RegistroID != 1 || otra[0].Intento != 2 {
		t.Fatalf("tras expirar debía volver el mismo con intento 2: %+v", otra)
	}
	if err := e.Cola.Borrar(ctx, otra[0].Recibo); err != nil {
		t.Fatal(err)
	}
	tercera, _ := e.Cola.Recibir(ctx, 10)
	if len(tercera) != 1 || tercera[0].RegistroID != 2 {
		t.Fatalf("debía llegar el segundo: %+v", tercera)
	}
}

func TestDynamoEscrituraCondicional(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	r := dominio.Resultado{RegistroID: time.Now().UnixNano(), Fecha: "2026-11-08", Proveedor: "a",
		Estado: dominio.EstadoOK, HTTPStatus: 200, Intentos: 1, Respuesta: []byte(`{}`), ProcesadoEn: time.Now()}
	if err := e.Resultados.Guardar(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := e.Resultados.Guardar(ctx, r); !errors.Is(err, resultados.ErrYaExiste) {
		t.Fatalf("segunda escritura debía fallar con ErrYaExiste, fue %v", err)
	}
	leido, ok, _ := e.Resultados.Leer(ctx, r.RegistroID, r.Fecha)
	if !ok || leido.Estado != dominio.EstadoOK {
		t.Fatalf("lectura: %+v %v", leido, ok)
	}
}

func TestPostgresFranjaIdempotente(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	ok, err := e.Fuente.IniciarFranja(ctx, "2026-11-08", 3)
	if err != nil || !ok {
		t.Fatalf("primera: %v %v", ok, err)
	}
	ok, err = e.Fuente.IniciarFranja(ctx, "2026-11-08", 3)
	if err != nil || ok {
		t.Fatalf("segunda debía devolver false: %v %v", ok, err)
	}
	_, _ = e.Pool.Exec(ctx, `INSERT INTO registros (proveedor, endpoint, body, slot) VALUES ('a','/x','{}',3),('b','/y','{}',3),('a','/z','{}',4)`)
	regs, err := e.Fuente.RegistrosDeSlot(ctx, 3)
	if err != nil || len(regs) != 2 {
		t.Fatalf("slot 3: %d registros, %v", len(regs), err)
	}
}
