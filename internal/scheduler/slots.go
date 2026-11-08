// Package scheduler decide qué registros se encolan en cada franja.
package scheduler

import (
	"sort"
	"time"

	"consultas-diarias/internal/dominio"
)

// AsignarSlots reparte los registros de cada proveedor de forma uniforme
// en las franjas del día: slot = rank * franjas / total, ordenando por id.
func AsignarSlots(regs []dominio.Registro, franjas int) map[int64]int {
	porProveedor := map[string][]int64{}
	for _, r := range regs {
		porProveedor[r.Proveedor] = append(porProveedor[r.Proveedor], r.ID)
	}
	out := make(map[int64]int, len(regs))
	for _, ids := range porProveedor {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		total := len(ids)
		for rank, id := range ids {
			out[id] = rank * franjas / total
		}
	}
	return out
}

const duracionFranja = 24 * time.Hour / dominio.FranjasPorDia

// SlotActual devuelve la franja y la fecha lógica para el instante t.
// Con escala 1 usa el reloj real. Con escala > 1 el día se comprime y
// arranca en inicio: t = inicio es slot 0 del día de inicio.
// En modo comprimido se asume t >= inicio. El día del sistema es UTC.
func SlotActual(t, inicio time.Time, escala float64) (int, string) {
	t = t.UTC()
	inicio = inicio.UTC()
	if escala <= 1 {
		desdeMedianoche := t.Sub(t.Truncate(24 * time.Hour))
		return int(desdeMedianoche / duracionFranja), t.Format("2006-01-02")
	}
	virtual := time.Duration(float64(t.Sub(inicio)) * escala)
	dias := int(virtual / (24 * time.Hour))
	resto := virtual % (24 * time.Hour)
	fecha := inicio.AddDate(0, 0, dias).Format("2006-01-02")
	return int(resto / duracionFranja), fecha
}
