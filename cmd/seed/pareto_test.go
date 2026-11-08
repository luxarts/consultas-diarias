package main

import "testing"

func TestRepartoParetoSumaYSesgo(t *testing.T) {
	r := repartoPareto(100000, 200, 1.1)
	total := 0
	for _, n := range r {
		total += n
	}
	if total != 100000 {
		t.Fatalf("suma %d", total)
	}
	top := 0
	for i := 0; i < 40; i++ { // 20 % de los proveedores
		top += r[i]
	}
	if top < 60000 {
		t.Fatalf("el 20 %% de los proveedores debía concentrar al menos 60 %% de registros, tiene %d", top)
	}
	for i, n := range r {
		if n < 1 {
			t.Fatalf("proveedor %d sin registros", i)
		}
	}
}
