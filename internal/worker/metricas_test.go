package worker

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/metricas"
)

// TestProcesarRegistraMetricas recorre los cuatro destinos posibles de un
// mensaje y verifica que cada uno quede contado con su estado y su código.
func TestProcesarRegistraMetricas(t *testing.T) {
	lector := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(lector))
	defer mp.Shutdown(context.Background())
	mets, err := metricas.Nueva(mp)
	if err != nil {
		t.Fatal(err)
	}

	casos := []struct {
		respuesta apiclient.Respuesta
		intento   int
		estado    string
		status    int
	}{
		{apiclient.Respuesta{Status: 200, Body: []byte(`{}`)}, 1, "ok", 200},
		{apiclient.Respuesta{Status: 400, Body: []byte(`{}`)}, 1, "fallo_definitivo", 400},
		{apiclient.Respuesta{Status: 503}, 1, "reintento", 503},
		{apiclient.Respuesta{Err: context.DeadlineExceeded}, dominio.IntentosMax, "agotado", 0},
	}
	for _, c := range casos {
		p, cola, _ := armar(c.respuesta)
		p.Metricas = mets
		m := entregar(t, cola)
		m.Intento = c.intento
		_ = p.Procesar(context.Background(), m)
	}

	var rm metricdata.ResourceMetrics
	if err := lector.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, c := range casos {
		if n := valor(t, rm, metricas.NombreResultado, attribute.String("proveedor", "acme"), attribute.String("estado", c.estado)); n != 1 {
			t.Errorf("resultado %s = %d, quiero 1", c.estado, n)
		}
		if n := valor(t, rm, metricas.NombreHTTP, attribute.String("proveedor", "acme"), attribute.Int("status", c.status)); n != 1 {
			t.Errorf("http %d = %d, quiero 1", c.status, n)
		}
	}
	if n := valor(t, rm, metricas.NombreEnVuelo); n != 0 {
		t.Errorf("en vuelo al terminar = %d, quiero 0", n)
	}
}

func valor(t *testing.T, rm metricdata.ResourceMetrics, nombre string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	quiero := attribute.NewSet(attrs...)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != nombre {
				continue
			}
			suma, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s no es un contador int64", nombre)
			}
			for _, p := range suma.DataPoints {
				if p.Attributes.Equals(&quiero) {
					return p.Value
				}
			}
			return 0
		}
	}
	t.Fatalf("métrica %q no encontrada", nombre)
	return 0
}
