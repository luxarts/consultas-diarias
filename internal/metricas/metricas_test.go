package metricas

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// armar devuelve métricas conectadas a un lector manual y una función que
// recolecta y devuelve lo acumulado.
func armar(t *testing.T) (*Metricas, func() metricdata.ResourceMetrics) {
	t.Helper()
	lector := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(lector))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := Nueva(mp)
	if err != nil {
		t.Fatal(err)
	}
	return m, func() metricdata.ResourceMetrics {
		var rm metricdata.ResourceMetrics
		if err := lector.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
		return rm
	}
}

func buscar(t *testing.T, rm metricdata.ResourceMetrics, nombre string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == nombre {
				return m
			}
		}
	}
	t.Fatalf("métrica %q no encontrada", nombre)
	return metricdata.Metrics{}
}

// contador devuelve el valor del punto cuyos atributos coinciden exactamente.
func contador(t *testing.T, rm metricdata.ResourceMetrics, nombre string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	m := buscar(t, rm, nombre)
	suma, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s no es un contador int64: %T", nombre, m.Data)
	}
	quiero := attribute.NewSet(attrs...)
	for _, p := range suma.DataPoints {
		if p.Attributes.Equals(&quiero) {
			return p.Value
		}
	}
	return 0
}

func TestConsultaRegistraStatusEstadoYLatencia(t *testing.T) {
	m, recolectar := armar(t)
	ctx := context.Background()
	m.Consulta(ctx, "acme", 200, "ok", 300*time.Millisecond)
	m.Consulta(ctx, "acme", 200, "ok", 100*time.Millisecond)
	m.Consulta(ctx, "acme", 503, "reintento", 50*time.Millisecond)
	m.Consulta(ctx, "otro", 400, "fallo_definitivo", 10*time.Millisecond)
	m.Consulta(ctx, "otro", 0, "agotado", 30*time.Second) // error de transporte

	rm := recolectar()
	prov := func(p string) attribute.KeyValue { return attribute.String("proveedor", p) }

	if n := contador(t, rm, "consultas_http", prov("acme"), attribute.Int("status", 200)); n != 2 {
		t.Errorf("http acme 200 = %d, quiero 2", n)
	}
	if n := contador(t, rm, "consultas_http", prov("acme"), attribute.Int("status", 503)); n != 1 {
		t.Errorf("http acme 503 = %d, quiero 1", n)
	}
	if n := contador(t, rm, "consultas_http", prov("otro"), attribute.Int("status", 0)); n != 1 {
		t.Errorf("http otro 0 = %d, quiero 1", n)
	}
	if n := contador(t, rm, "consultas_resultado", prov("acme"), attribute.String("estado", "ok")); n != 2 {
		t.Errorf("resultado acme ok = %d, quiero 2", n)
	}
	if n := contador(t, rm, "consultas_resultado", prov("acme"), attribute.String("estado", "reintento")); n != 1 {
		t.Errorf("resultado acme reintento = %d, quiero 1", n)
	}
	if n := contador(t, rm, "consultas_resultado", prov("otro"), attribute.String("estado", "agotado")); n != 1 {
		t.Errorf("resultado otro agotado = %d, quiero 1", n)
	}

	h, ok := buscar(t, rm, "consultas_latencia_segundos").Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatal("latencia no es un histograma float64")
	}
	quiero := attribute.NewSet(prov("acme"))
	for _, p := range h.DataPoints {
		if p.Attributes.Equals(&quiero) {
			if p.Count != 3 || p.Sum < 0.449 || p.Sum > 0.451 {
				t.Errorf("latencia acme: count=%d sum=%.3f, quiero 3 y 0.450", p.Count, p.Sum)
			}
			return
		}
	}
	t.Error("sin punto de latencia para acme")
}

func TestEnVueloSubeYBaja(t *testing.T) {
	m, recolectar := armar(t)
	ctx := context.Background()
	m.Entrar(ctx)
	m.Entrar(ctx)
	m.Salir(ctx)
	if n := contador(t, recolectar(), "consultas_en_vuelo"); n != 1 {
		t.Errorf("en vuelo = %d, quiero 1", n)
	}
}

func TestNilNoHaceNada(t *testing.T) {
	var m *Metricas
	m.Consulta(context.Background(), "x", 200, "ok", time.Second)
	m.Entrar(context.Background())
	m.Salir(context.Background())
}
