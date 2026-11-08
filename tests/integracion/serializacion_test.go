//go:build integration

package integracion

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/worker"
)

type stats struct {
	MaxEnVuelo  map[string]int `json:"max_en_vuelo"`
	Violaciones int            `json:"violaciones"`
}

func leerStats(t *testing.T, base string) stats {
	t.Helper()
	resp, err := http.Get(base + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s stats
	_ = json.NewDecoder(resp.Body).Decode(&s)
	return s
}

// Un proveedor, 20 mensajes, 5 workers con concurrencia 4 cada uno.
// La API mock responde 409 si recibe dos requests simultáneos del mismo
// proveedor; el test pasa sólo si eso nunca ocurre.
func TestNuncaDosRequestsAlMismoProveedor(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fecha := fechaUnica()

	var msgs []dominio.Mensaje
	for i := int64(1); i <= 20; i++ {
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: "unico", Endpoint: "/c", Body: []byte(`{}`), Fecha: fecha})
	}
	for i := int64(21); i <= 40; i++ {
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: "otro", Endpoint: "/c", Body: []byte(`{}`), Fecha: fecha})
	}
	if err := e.Cola.Enviar(ctx, msgs); err != nil {
		t.Fatal(err)
	}

	antes := leerStats(t, e.APIBase)
	for i := 0; i < 5; i++ {
		p := &worker.Procesador{Cola: e.Cola, Resultados: e.Resultados,
			API: apiclient.NuevoHTTP(e.APIBase, dominio.TimeoutHTTP), Ahora: time.Now, Rnd: rand.Float64, Escala: 1}
		go worker.Correr(ctx, p, 4)
	}

	esperar(t, 60*time.Second, func() bool {
		_, ok1, _ := e.Resultados.Leer(ctx, 20, fecha)
		_, ok2, _ := e.Resultados.Leer(ctx, 40, fecha)
		return ok1 && ok2
	})
	cancel()

	despues := leerStats(t, e.APIBase)
	if despues.Violaciones != antes.Violaciones {
		t.Fatalf("hubo %d violaciones de concurrencia", despues.Violaciones-antes.Violaciones)
	}
	if despues.MaxEnVuelo["unico"] > 1 || despues.MaxEnVuelo["otro"] > 1 {
		t.Fatalf("máximo en vuelo superó 1: %+v", despues.MaxEnVuelo)
	}
	for i := int64(1); i <= 40; i++ {
		r, ok, _ := e.Resultados.Leer(ctx, i, fecha)
		if !ok || r.Estado != dominio.EstadoOK {
			t.Fatalf("registro %d: %+v %v", i, r, ok)
		}
	}
}
