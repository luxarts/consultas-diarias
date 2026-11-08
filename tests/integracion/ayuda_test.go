//go:build integration

package integracion

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	colaSQS := cola.NuevaSQS(sq, base+"consultas.fifo")
	colaSQS.Espera = time.Second // apagar un worker no debe demorar 20 s
	e := &entorno{
		Cola:       colaSQS,
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

// purgar vacía colas y tablas para que cada test arranque limpio. Las colas
// se drenan con receive + delete en lugar de PurgeQueue, que en AWS admite
// una llamada por minuto y es asíncrono. Primero la principal: recibir ahí un
// mensaje que ya agotó sus recepciones lo mueve a la DLQ, que se drena
// después. Los mensajes en vuelo de otro consumidor no se pueden drenar.
func (e *entorno) purgar(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, u := range []string{e.ColaURL, e.DLQURL} {
		e.drenar(t, ctx, u)
	}
	if _, err := e.Pool.Exec(ctx, `TRUNCATE registros, corridas_diarias, proveedores_estado`); err != nil {
		t.Logf("truncate: %v", err)
	}
}

// drenar recibe y borra hasta que dos receives seguidos vuelven vacíos.
func (e *entorno) drenar(t *testing.T, ctx context.Context, url string) {
	t.Helper()
	vacios := 0
	for i := 0; vacios < 2 && i < 1000; i++ {
		out, err := e.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Logf("drenar %s: %v", url, err)
			return
		}
		if len(out.Messages) == 0 {
			vacios++
			continue
		}
		vacios = 0
		for _, m := range out.Messages {
			if _, err := e.SQS.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Logf("drenar %s: borrar: %v", url, err)
			}
		}
	}
}

// mensajesVisibles devuelve ApproximateNumberOfMessages de la cola, o -1 si
// no se pudo leer.
func (e *entorno) mensajesVisibles(ctx context.Context, url string) int {
	out, err := e.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		return -1
	}
	n, err := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	if err != nil {
		return -1
	}
	return n
}

// fechaUnica devuelve una "fecha" lógica distinta por corrida para que el
// MessageDeduplicationId (id:fecha) no colisione con corridas anteriores
// dentro de la ventana de 5 minutos de SQS FIFO.
func fechaUnica() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000") }

// fechaDiaUnica es como fechaUnica pero con formato YYYY-MM-DD, válido para
// la columna DATE de corridas_diarias: un día distinto por cada segundo.
func fechaDiaUnica() string {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.AddDate(0, 0, int(time.Now().Unix()%1_000_000)).Format("2006-01-02")
}

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
