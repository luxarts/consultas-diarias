package metricas

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Exportador conecta el worker al colector. Lee la configuración estándar de
// OpenTelemetry del entorno: OTEL_EXPORTER_OTLP_ENDPOINT (host:puerto del
// colector; si falta, no se exporta nada y Metricas queda nil) y
// OTEL_SERVICE_NAME. El intervalo de envío es corto porque la demo comprimida
// dura minutos; en producción 60 s alcanza.
type Exportador struct {
	mp *sdk.MeterProvider
}

// Conectar devuelve las métricas y el exportador, o (nil, nil, nil) si el
// entorno no define un colector.
func Conectar(ctx context.Context, intervalo time.Duration) (*Metricas, *Exportador, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return nil, nil, nil
	}
	exp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, nil, fmt.Errorf("exportador OTLP: %w", err)
	}
	host, _ := os.Hostname()
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(nombreServicio()),
			semconv.ServiceInstanceID(host),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("recurso: %w", err)
	}
	mp := sdk.NewMeterProvider(
		sdk.WithResource(res),
		sdk.WithReader(sdk.NewPeriodicReader(exp, sdk.WithInterval(intervalo))),
	)
	m, err := Nueva(mp)
	if err != nil {
		_ = mp.Shutdown(ctx)
		return nil, nil, err
	}
	return m, &Exportador{mp: mp}, nil
}

// Cerrar envía lo que quede acumulado y libera la conexión.
func (e *Exportador) Cerrar(ctx context.Context) error {
	if e == nil {
		return nil
	}
	return e.mp.Shutdown(ctx)
}

func nombreServicio() string {
	if n := os.Getenv("OTEL_SERVICE_NAME"); n != "" {
		return n
	}
	return "worker"
}
