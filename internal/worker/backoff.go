package worker

import (
	"time"

	"consultas-diarias/internal/dominio"
)

var esperas = []time.Duration{
	30 * time.Second,
	2 * time.Minute,
	8 * time.Minute,
	30 * time.Minute,
}

// Backoff devuelve cuánto esperar antes del siguiente intento y si
// corresponde reintentar. El jitter es ±20 % para que los reintentos de
// distintos proveedores no se alineen.
func Backoff(intento int, rnd func() float64) (time.Duration, bool) {
	if intento < 1 || intento >= dominio.IntentosMax {
		return 0, false
	}
	base := esperas[intento-1]
	factor := 0.8 + 0.4*rnd()
	return time.Duration(float64(base) * factor), true
}
