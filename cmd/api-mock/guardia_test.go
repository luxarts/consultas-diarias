package main

import (
	"sync"
	"testing"
)

func TestGuardiaPermiteHastaElLimite(t *testing.T) {
	g := nuevaGuardia(1)
	if !g.entrar("a") {
		t.Fatal("primera entrada debía pasar")
	}
	if g.entrar("a") {
		t.Fatal("segunda entrada simultánea debía rechazarse")
	}
	if !g.entrar("b") {
		t.Fatal("otro proveedor no se ve afectado")
	}
	g.salir("a")
	if !g.entrar("a") {
		t.Fatal("tras salir debía volver a pasar")
	}
	s := g.stats()
	if s.Violaciones != 1 || s.MaxEnVuelo["a"] != 1 || s.MaxEnVuelo["b"] != 1 {
		t.Fatalf("stats inesperadas: %+v", s)
	}
}

func TestGuardiaConcurrente(t *testing.T) {
	g := nuevaGuardia(1)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.entrar("a") {
				g.salir("a")
			}
		}()
	}
	wg.Wait()
	if g.stats().MaxEnVuelo["a"] != 1 {
		t.Fatalf("el máximo observado nunca debe superar 1: %+v", g.stats())
	}
}
