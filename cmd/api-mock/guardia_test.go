package main

import (
	"sync"
	"testing"
)

func TestGuardiaSecuenciaForzada(t *testing.T) {
	g := nuevaGuardia(1)
	got := []string{}
	for i := 0; i < 4; i++ {
		code, ok := g.siguienteForzado("p", "503, 500,200")
		if !ok {
			got = append(got, "agotada")
			continue
		}
		got = append(got, code)
	}
	quiero := []string{"503", "500", "200", "agotada"}
	for i := range quiero {
		if got[i] != quiero[i] {
			t.Fatalf("secuencia = %v, quiero %v", got, quiero)
		}
	}
	if code, ok := g.siguienteForzado("otro", "429"); !ok || code != "429" {
		t.Fatalf("otro proveedor debía tener su propia secuencia: %q %v", code, ok)
	}
}

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
