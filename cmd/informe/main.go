// informe resume la corrida de un día: resultados por estado, latencias,
// saturación estimada y pendientes por proveedor, profundidad de las colas y
// las estadísticas de concurrencia del mock.
//
// Entorno: TABLA_RESULTADOS, POSTGRES_URL, API_BASE_URL, ESCALA_TIEMPO
// (1 si falta) y FECHA (YYYY-MM-DD; si falta usa la fecha más vieja que haya
// en la tabla de resultados).
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/awscfg"
)

const umbralSaturacion = 0.8

type fila struct {
	Fecha      string `dynamodbav:"fecha"`
	Proveedor  string `dynamodbav:"proveedor"`
	Estado     string `dynamodbav:"estado"`
	LatenciaMs int64  `dynamodbav:"latencia_ms"`
	Intentos   int    `dynamodbav:"intentos"`
}

// estadisticas replica la respuesta de GET /stats del mock.
type estadisticas struct {
	Violaciones      int            `json:"violaciones"`
	Requests         int            `json:"requests"`
	MaxEnVueloGlobal int            `json:"max_en_vuelo_global"`
	MaxEnVuelo       map[string]int `json:"max_en_vuelo"`
}

func fallar(contexto string, err error) {
	fmt.Fprintf(os.Stderr, "informe: %s: %v\n", contexto, err)
	os.Exit(1)
}

func main() {
	ctx := context.Background()
	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		fallar("config aws", err)
	}
	escala, _ := strconv.ParseFloat(os.Getenv("ESCALA_TIEMPO"), 64)
	if escala < 1 {
		escala = 1
	}

	filas, fecha := leerResultados(ctx, dynamodb.NewFromConfig(cfg), os.Getenv("FECHA"))
	if fecha == "" {
		fmt.Println("No hay resultados en la tabla.")
	} else {
		fmt.Printf("Fecha: %s (escala de tiempo %g)\n\n", fecha, escala)
	}
	k := activosPorProveedor(ctx, os.Getenv("POSTGRES_URL"))

	resumenResultados(filas)
	saturacion(filas, k, escala)
	colas(ctx, sqs.NewFromConfig(cfg))
	statsMock(os.Getenv("API_BASE_URL"))
}

// leerResultados recorre la tabla y se queda con las filas de la fecha
// pedida, o de la más vieja si no se pidió ninguna.
func leerResultados(ctx context.Context, ddb *dynamodb.Client, fecha string) ([]fila, string) {
	var todas []fila
	pag := dynamodb.NewScanPaginator(ddb, &dynamodb.ScanInput{
		TableName:            aws.String(os.Getenv("TABLA_RESULTADOS")),
		ProjectionExpression: aws.String("#f, #p, #e, #l, #i"),
		ExpressionAttributeNames: map[string]string{
			"#f": "fecha", "#p": "proveedor", "#e": "estado", "#l": "latencia_ms", "#i": "intentos",
		},
	})
	for pag.HasMorePages() {
		out, err := pag.NextPage(ctx)
		if err != nil {
			fallar("scan de resultados", err)
		}
		var parte []fila
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &parte); err != nil {
			fallar("leer resultados", err)
		}
		todas = append(todas, parte...)
	}
	if fecha == "" {
		for _, f := range todas {
			if fecha == "" || f.Fecha < fecha {
				fecha = f.Fecha
			}
		}
	}
	var filas []fila
	for _, f := range todas {
		if f.Fecha == fecha {
			filas = append(filas, f)
		}
	}
	return filas, fecha
}

// activosPorProveedor devuelve K: los registros activos de cada proveedor en
// la tabla fuente, que es lo que debería procesarse en el día. Contar los
// resultados subestimaría justo a los proveedores que no terminan.
func activosPorProveedor(ctx context.Context, url string) map[string]int {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fallar("postgres", err)
	}
	defer pool.Close()
	filas, err := pool.Query(ctx, `SELECT proveedor, count(*) FROM registros WHERE activo GROUP BY 1`)
	if err != nil {
		fallar("contar registros", err)
	}
	defer filas.Close()
	k := map[string]int{}
	for filas.Next() {
		var prov string
		var n int
		if err := filas.Scan(&prov, &n); err != nil {
			fallar("contar registros", err)
		}
		k[prov] = n
	}
	if err := filas.Err(); err != nil {
		fallar("contar registros", err)
	}
	return k
}

func resumenResultados(filas []fila) {
	porEstado := map[string]int{}
	lat := make([]int64, 0, len(filas))
	for _, f := range filas {
		porEstado[f.Estado]++
		lat = append(lat, f.LatenciaMs)
	}
	slices.Sort(lat)
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
	fmt.Printf("Latencia medida p50 %d ms, p95 %d ms\n\n", pct(0.5), pct(0.95))
}

type proveedor struct {
	nombre     string
	k          int     // registros activos
	resultados int     // resultados de la fecha
	lMs        float64 // latencia efectiva media: latencia × intentos
	s          float64 // saturación en tiempo real
}

// saturacion estima s = K × L × escala / 86400 por proveedor. L se mide en el
// día comprimido; multiplicarla por la escala la lleva a tiempo real, porque
// el día comprimido dura 86400 / escala segundos.
func saturacion(filas []fila, k map[string]int, escala float64) {
	type acum struct {
		n     int
		sumMs int64
	}
	porProv := map[string]*acum{}
	for _, f := range filas {
		a := porProv[f.Proveedor]
		if a == nil {
			a = &acum{}
			porProv[f.Proveedor] = a
		}
		a.n++
		a.sumMs += f.LatenciaMs * int64(f.Intentos)
	}
	nombres := map[string]bool{}
	for p := range k {
		nombres[p] = true
	}
	for p := range porProv {
		nombres[p] = true
	}
	var provs []proveedor
	pendientesTotal, conPendientes, saturados := 0, 0, 0
	for p := range nombres {
		pr := proveedor{nombre: p, k: k[p]}
		if a := porProv[p]; a != nil {
			pr.resultados = a.n
			pr.lMs = float64(a.sumMs) / float64(a.n)
		}
		pr.s = float64(pr.k) * pr.lMs * escala / 1000 / 86400
		if pend := pr.k - pr.resultados; pend > 0 {
			pendientesTotal += pend
			conPendientes++
		}
		if pr.s > umbralSaturacion {
			saturados++
		}
		provs = append(provs, pr)
	}
	slices.SortFunc(provs, func(a, b proveedor) int {
		if c := cmp.Compare(b.s, a.s); c != 0 {
			return c
		}
		return cmp.Compare(a.nombre, b.nombre)
	})

	fmt.Printf("Saturación por proveedor, s = K × L × escala / 86400 (L = latencia × intentos, K de Postgres)\n")
	fmt.Printf("Proveedores: %d, con s > %.1f: %d, con pendientes: %d, pendientes totales: %d\n",
		len(provs), umbralSaturacion, saturados, conPendientes, pendientesTotal)
	fmt.Printf("  %-16s %7s %10s %10s %8s %6s\n", "proveedor", "K", "resultados", "pendientes", "L ms", "s")
	for i, p := range provs {
		if i >= 10 && p.s <= umbralSaturacion && p.k-p.resultados <= 0 {
			continue
		}
		marca := ""
		if p.s > umbralSaturacion {
			marca = "  > 0.8"
		}
		fmt.Printf("  %-16s %7d %10d %10d %8.1f %6.2f%s\n", p.nombre, p.k, p.resultados, p.k-p.resultados, p.lMs, p.s, marca)
	}
	fmt.Println("  (se listan los 10 de mayor s y cualquier otro saturado o con pendientes)")
	fmt.Println()
}

func colas(ctx context.Context, sq *sqs.Client) {
	fmt.Println("Colas:")
	for _, nombre := range []string{"consultas.fifo", "consultas-dlq.fifo"} {
		u, err := sq.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(nombre)})
		if err != nil {
			fallar("GetQueueUrl "+nombre, err)
		}
		out, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: u.QueueUrl,
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			fallar("GetQueueAttributes "+nombre, err)
		}
		fmt.Printf("  %-20s visibles=%s en_vuelo=%s\n", nombre,
			out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)],
			out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	}
	fmt.Println()
}

func statsMock(base string) {
	resp, err := http.Get(base + "/stats")
	if err != nil {
		fallar("stats del mock", err)
	}
	defer resp.Body.Close()
	var s estadisticas
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		fallar("stats del mock", err)
	}
	maxProv := 0
	var excedidos []string
	for p, m := range s.MaxEnVuelo {
		maxProv = max(maxProv, m)
		if m > 1 {
			excedidos = append(excedidos, fmt.Sprintf("%s=%d", p, m))
		}
	}
	slices.Sort(excedidos)
	fmt.Println("Mock de la API (desde su arranque):")
	fmt.Printf("  violaciones                       %d\n", s.Violaciones)
	fmt.Printf("  requests                          %d\n", s.Requests)
	fmt.Printf("  máximo en vuelo, todos los prov.  %d\n", s.MaxEnVueloGlobal)
	fmt.Printf("  máximo en vuelo de un proveedor   %d\n", maxProv)
	if len(excedidos) == 0 {
		fmt.Println("  proveedores con máximo > 1        ninguno")
	} else {
		fmt.Printf("  proveedores con máximo > 1        %v\n", excedidos)
	}
}
