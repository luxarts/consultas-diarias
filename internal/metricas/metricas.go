// Package metricas define los instrumentos de observabilidad del worker.
//
// Son las métricas custom de la spec (§9) expresadas con OpenTelemetry: el
// worker las empuja por OTLP a un colector y no sabe nada de cuántas réplicas
// hay ni de quién las consume. En la POC el colector las exporta a Prometheus
// y Grafana las grafica; en AWS el mismo colector (ADOT como sidecar en
// Fargate) las escribiría en CloudWatch con dimensión `proveedor`.
package metricas

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Nombres de los instrumentos. El colector los publica tal cual (sin sufijos
// `_total` ni de unidad); el tablero de Grafana los consulta por estos nombres.
const (
	NombreHTTP      = "consultas_http"
	NombreResultado = "consultas_resultado"
	NombreLatencia  = "consultas_latencia_segundos"
	NombreEnVuelo   = "consultas_en_vuelo"
)

// Metricas agrupa los instrumentos. Un puntero nil es válido y no registra
// nada, para que el Procesador se pueda armar sin observabilidad en tests.
type Metricas struct {
	http      metric.Int64Counter
	resultado metric.Int64Counter
	latencia  metric.Float64Histogram
	enVuelo   metric.Int64UpDownCounter
}

// Nueva crea los instrumentos sobre el proveedor dado.
func Nueva(mp metric.MeterProvider) (*Metricas, error) {
	meter := mp.Meter("consultas-diarias/worker")
	var m Metricas
	var err error
	if m.http, err = meter.Int64Counter(NombreHTTP,
		metric.WithDescription("Requests a la API por proveedor y código HTTP (0 = error de transporte)")); err != nil {
		return nil, err
	}
	if m.resultado, err = meter.Int64Counter(NombreResultado,
		metric.WithDescription("Destino de cada intento: ok, fallo_definitivo, reintento o agotado")); err != nil {
		return nil, err
	}
	if m.latencia, err = meter.Float64Histogram(NombreLatencia,
		metric.WithDescription("Latencia de la API por proveedor"),
		metric.WithUnit("s"),
		// Cubre desde los ~2 ms del mock en la demo comprimida hasta el timeout de 30 s.
		metric.WithExplicitBucketBoundaries(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)); err != nil {
		return nil, err
	}
	if m.enVuelo, err = meter.Int64UpDownCounter(NombreEnVuelo,
		metric.WithDescription("Mensajes en proceso en este worker")); err != nil {
		return nil, err
	}
	return &m, nil
}

// Consulta registra un intento terminado: el código HTTP recibido (0 si no
// hubo respuesta), el destino del mensaje y la latencia de la API.
func (m *Metricas) Consulta(ctx context.Context, proveedor string, status int, estado string, latencia time.Duration) {
	if m == nil {
		return
	}
	prov := attribute.String("proveedor", proveedor)
	m.http.Add(ctx, 1, metric.WithAttributes(prov, attribute.Int("status", status)))
	m.resultado.Add(ctx, 1, metric.WithAttributes(prov, attribute.String("estado", estado)))
	m.latencia.Record(ctx, latencia.Seconds(), metric.WithAttributes(prov))
}

// Entrar y Salir delimitan un mensaje en proceso.
func (m *Metricas) Entrar(ctx context.Context) {
	if m != nil {
		m.enVuelo.Add(ctx, 1)
	}
}

func (m *Metricas) Salir(ctx context.Context) {
	if m != nil {
		m.enVuelo.Add(ctx, -1)
	}
}
