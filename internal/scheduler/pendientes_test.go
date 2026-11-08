package scheduler

import (
	"reflect"
	"testing"
)

func TestFranjasAPublicar(t *testing.T) {
	casos := []struct {
		nombre      string
		ultimaFecha string
		ultimoSlot  int
		fecha       string
		slot        int
		quiero      []Franja
	}{
		{"al arrancar sólo la actual", "", -1, "2026-11-08", 40, []Franja{{"2026-11-08", 40}}},
		{"misma franja no repite", "2026-11-08", 40, "2026-11-08", 40, nil},
		{"siguiente franja", "2026-11-08", 40, "2026-11-08", 41, []Franja{{"2026-11-08", 41}}},
		{"tick atrasado recupera las intermedias", "2026-11-08", 40, "2026-11-08", 43,
			[]Franja{{"2026-11-08", 41}, {"2026-11-08", 42}, {"2026-11-08", 43}}},
		{"cambio de fecha completa el día y reinicia en 0", "2026-11-08", 286, "2026-11-09", 1,
			[]Franja{{"2026-11-08", 287}, {"2026-11-09", 0}, {"2026-11-09", 1}}},
		{"cambio de fecha con el día completo", "2026-11-08", 287, "2026-11-09", 0, []Franja{{"2026-11-09", 0}}},
		{"reloj hacia atrás no publica", "2026-11-09", 3, "2026-11-08", 287, nil},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			got := FranjasAPublicar(c.ultimaFecha, c.ultimoSlot, c.fecha, c.slot)
			if !reflect.DeepEqual(got, c.quiero) {
				t.Fatalf("got %v, quiero %v", got, c.quiero)
			}
		})
	}
}
