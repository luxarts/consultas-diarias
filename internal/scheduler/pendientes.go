package scheduler

import "consultas-diarias/internal/dominio"

// Franja identifica una franja de un día lógico.
type Franja struct {
	Fecha string // YYYY-MM-DD
	Slot  int
}

// FranjasAPublicar devuelve, en orden, las franjas que faltan publicar desde
// la última publicada por este proceso hasta la actual inclusive. Cubre el
// caso de un tick atrasado o de un EncolarFranja que tardó más que una
// franja: ninguna franja intermedia se saltea.
//
//   - Sin franja previa (ultimaFecha vacía, recién arrancado) devuelve sólo
//     la actual: al arrancar no se recuperan franjas pasadas.
//   - Si cambió la fecha, completa el día anterior hasta la franja 287 y
//     sigue desde la 0 de la fecha nueva.
//   - Si el reloj retrocede no devuelve nada.
func FranjasAPublicar(ultimaFecha string, ultimoSlot int, fecha string, slot int) []Franja {
	if ultimaFecha == "" {
		return []Franja{{fecha, slot}}
	}
	var out []Franja
	switch {
	case fecha == ultimaFecha:
		for s := ultimoSlot + 1; s <= slot; s++ {
			out = append(out, Franja{fecha, s})
		}
	case fecha > ultimaFecha:
		for s := ultimoSlot + 1; s < dominio.FranjasPorDia; s++ {
			out = append(out, Franja{ultimaFecha, s})
		}
		for s := 0; s <= slot; s++ {
			out = append(out, Franja{fecha, s})
		}
	}
	return out
}
