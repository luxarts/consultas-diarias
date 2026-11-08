//go:build integration

package integracion

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

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
	enviarUno(t, e, 101, "reint-ok", "503,500,200", fecha)
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
	enviarUno(t, e, 102, "fallo-400", "400", fecha)
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
	enviarUno(t, e, 103, "siempre-503", "503,503,503,503,503,503", fecha)
	lanzarWorker(ctx, e, 600)
	esperar(t, 60*time.Second, func() bool {
		r, ok, _ := e.Resultados.Leer(ctx, 103, fecha)
		return ok && r.Estado == dominio.EstadoAgotado
	})
	r, _, _ := e.Resultados.Leer(ctx, 103, fecha)
	if r.Intentos != dominio.IntentosMax {
		t.Fatalf("intentos = %d", r.Intentos)
	}
	// Tras el quinto intento el mensaje queda en vuelo hasta que vence el
	// visibility timeout (60 s); recién entonces SQS lo mueve a la DLQ.
	esperar(t, 90*time.Second, func() bool {
		out, err := e.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(e.DLQURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
		})
		return err == nil && out.Attributes["ApproximateNumberOfMessages"] == "1"
	})
}
