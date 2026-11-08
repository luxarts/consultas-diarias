// informe resume el estado de una corrida: resultados por estado, latencias,
// saturación estimada por proveedor y mensajes pendientes.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"consultas-diarias/internal/awscfg"
)

type fila struct {
	Proveedor  string `dynamodbav:"proveedor"`
	Estado     string `dynamodbav:"estado"`
	LatenciaMs int64  `dynamodbav:"latencia_ms"`
	Intentos   int    `dynamodbav:"intentos"`
}

func main() {
	ctx := context.Background()
	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		panic(err)
	}
	ddb := dynamodb.NewFromConfig(cfg)
	var filas []fila
	pag := dynamodb.NewScanPaginator(ddb, &dynamodb.ScanInput{
		TableName:            aws.String(os.Getenv("TABLA_RESULTADOS")),
		ProjectionExpression: aws.String("proveedor, estado, latencia_ms, intentos"),
	})
	for pag.HasMorePages() {
		out, err := pag.NextPage(ctx)
		if err != nil {
			panic(err)
		}
		var parte []fila
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &parte); err != nil {
			panic(err)
		}
		filas = append(filas, parte...)
	}

	porEstado := map[string]int{}
	var lat []int64
	type acum struct {
		n     int
		sumMs int64
	}
	porProv := map[string]*acum{}
	for _, f := range filas {
		porEstado[f.Estado]++
		lat = append(lat, f.LatenciaMs)
		a := porProv[f.Proveedor]
		if a == nil {
			a = &acum{}
			porProv[f.Proveedor] = a
		}
		a.n++
		a.sumMs += f.LatenciaMs * int64(f.Intentos) // latencia efectiva incluyendo reintentos
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) int64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)]
	}

	fmt.Printf("Resultados: %d\n", len(filas))
	for _, e := range []string{"ok", "fallo_definitivo", "agotado"} {
		fmt.Printf("  %-18s %d\n", e, porEstado[e])
	}
	fmt.Printf("Latencia p50 %d ms, p95 %d ms\n", pct(0.5), pct(0.95))

	type sat struct {
		prov string
		k    int
		lMs  int64
		s    float64
	}
	var sats []sat
	for p, a := range porProv {
		l := a.sumMs / int64(a.n)
		// saturacion = K * L / 86400 s, con L la latencia efectiva media.
		sats = append(sats, sat{p, a.n, l, float64(a.n) * float64(l) / 1000 / 86400})
	}
	sort.Slice(sats, func(i, j int) bool { return sats[i].s > sats[j].s })
	fmt.Println("Proveedores con mayor saturación estimada (K × L / 86400):")
	for i := 0; i < len(sats) && i < 5; i++ {
		fmt.Printf("  %-16s K=%-6d L=%-5d ms  saturación=%.3f\n", sats[i].prov, sats[i].k, sats[i].lMs, sats[i].s)
	}

	sq := sqs.NewFromConfig(cfg)
	for _, nombre := range []string{"consultas.fifo", "consultas-dlq.fifo"} {
		url := os.Getenv("AWS_ENDPOINT_URL") + "/000000000000/" + nombre
		out, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%-20s visibles=%s en_vuelo=%s\n", nombre,
			out.Attributes["ApproximateNumberOfMessages"], out.Attributes["ApproximateNumberOfMessagesNotVisible"])
	}
}
