package worker

import (
	"testing"
	"time"
)

func TestBackoffBase(t *testing.T) {
	medio := func() float64 { return 0.5 } // jitter neutro
	casos := []struct {
		intento int
		espera  time.Duration
		sigue   bool
	}{
		{1, 30 * time.Second, true},
		{2, 2 * time.Minute, true},
		{3, 8 * time.Minute, true},
		{4, 30 * time.Minute, true},
		{5, 0, false},
		{9, 0, false},
	}
	for _, c := range casos {
		espera, sigue := Backoff(c.intento, medio)
		if espera != c.espera || sigue != c.sigue {
			t.Errorf("Backoff(%d) = (%v, %v), quiero (%v, %v)", c.intento, espera, sigue, c.espera, c.sigue)
		}
	}
}

func TestBackoffJitterDentroDelRango(t *testing.T) {
	min, _ := Backoff(2, func() float64 { return 0 })
	max, _ := Backoff(2, func() float64 { return 0.999999 })
	base := 2 * time.Minute
	if min < time.Duration(float64(base)*0.8) || min > base {
		t.Errorf("jitter mínimo fuera de rango: %v", min)
	}
	if max > time.Duration(float64(base)*1.2) || max < base {
		t.Errorf("jitter máximo fuera de rango: %v", max)
	}
}
