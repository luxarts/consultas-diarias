package scheduler

import (
	"testing"
	"time"

	"consultas-diarias/internal/dominio"
)

func TestAsignarSlotsUniformePorProveedor(t *testing.T) {
	var regs []dominio.Registro
	id := int64(1)
	for i := 0; i < 2880; i++ { // proveedor grande: 10 por franja
		regs = append(regs, dominio.Registro{ID: id, Proveedor: "grande"})
		id++
	}
	for i := 0; i < 3; i++ { // proveedor chico: 3 en todo el día
		regs = append(regs, dominio.Registro{ID: id, Proveedor: "chico"})
		id++
	}
	slots := AsignarSlots(regs, 288)

	porFranja := map[int]int{}
	for _, r := range regs {
		if r.Proveedor == "grande" {
			porFranja[slots[r.ID]]++
		}
	}
	for f := 0; f < 288; f++ {
		if porFranja[f] != 10 {
			t.Fatalf("franja %d del proveedor grande tiene %d, quiero 10", f, porFranja[f])
		}
	}
	chico := []int{slots[2881], slots[2882], slots[2883]}
	if chico[0] != 0 || chico[1] != 96 || chico[2] != 192 {
		t.Fatalf("proveedor chico mal repartido: %v", chico)
	}
}

func TestSlotActualSinEscala(t *testing.T) {
	cualquiera := time.Time{}
	slot, fecha := SlotActual(time.Date(2026, 11, 8, 13, 7, 0, 0, time.UTC), cualquiera, 1)
	if slot != 157 || fecha != "2026-11-08" {
		t.Fatalf("got %d %s", slot, fecha)
	}
	slot, _ = SlotActual(time.Date(2026, 11, 8, 23, 59, 59, 0, time.UTC), cualquiera, 1)
	if slot != 287 {
		t.Fatalf("got %d", slot)
	}
}

func TestSlotActualComprimido(t *testing.T) {
	inicio := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	// escala 150: una franja de 300 s dura 2 s; el día dura 576 s.
	slot, fecha := SlotActual(inicio.Add(5*time.Second), inicio, 150)
	if slot != 2 || fecha != "2026-11-08" {
		t.Fatalf("got %d %s", slot, fecha)
	}
	slot, fecha = SlotActual(inicio.Add(600*time.Second), inicio, 150)
	if slot != 12 || fecha != "2026-11-09" {
		t.Fatalf("segundo día comprimido: got %d %s", slot, fecha)
	}
}
