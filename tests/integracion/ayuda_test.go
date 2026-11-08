//go:build integration

package integracion

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/awscfg"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/fuente"
	"consultas-diarias/internal/resultados"
)

type entorno struct {
	Cola       *cola.SQS
	Resultados *resultados.Dynamo
	Fuente     *fuente.Postgres
	Pool       *pgxpool.Pool
	SQS        *sqs.Client
	ColaURL    string
	DLQURL     string
	APIBase    string
}

func armarEntorno(t *testing.T) *entorno {
	t.Helper()
	ctx := context.Background()
	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		t.Fatal(err)
	}
	base := os.Getenv("AWS_ENDPOINT_URL") + "/000000000000/"
	sq := sqs.NewFromConfig(cfg)
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e := &entorno{
		Cola:       cola.NuevaSQS(sq, base+"consultas.fifo"),
		Resultados: resultados.NuevaDynamo(dynamodb.NewFromConfig(cfg), "resultados"),
		Fuente:     fuente.NuevaPostgres(pool),
		Pool:       pool,
		SQS:        sq,
		ColaURL:    base + "consultas.fifo",
		DLQURL:     base + "consultas-dlq.fifo",
		APIBase:    os.Getenv("API_BASE_URL"),
	}
	e.purgar(t)
	return e
}

// purgar vacía colas y tablas para que cada test arranque limpio.
func (e *entorno) purgar(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, u := range []string{e.ColaURL, e.DLQURL} {
		_, _ = e.SQS.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &u})
	}
	_, _ = e.Pool.Exec(ctx, `TRUNCATE registros, corridas_diarias, proveedores_estado`)
	time.Sleep(time.Second) // PurgeQueue es asíncrono
}

// fechaUnica devuelve una "fecha" lógica distinta por corrida para que el
// MessageDeduplicationId (id-fecha) no colisione con corridas anteriores
// dentro de la ventana de 5 minutos de SQS FIFO.
func fechaUnica() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000") }

func esperar(t *testing.T, plazo time.Duration, cond func() bool) {
	t.Helper()
	limite := time.Now().Add(plazo)
	for time.Now().Before(limite) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("condición no alcanzada en el plazo")
}
