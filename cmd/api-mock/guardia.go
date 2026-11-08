package main

import "sync"

// guardia cuenta requests en vuelo por proveedor y registra el máximo
// observado. Es la aserción empírica de "no concurrencia por proveedor".
type guardia struct {
	mu          sync.Mutex
	limite      int
	enVuelo     map[string]int
	maxEnVuelo  map[string]int
	violaciones int
	requests    int
}

type estadisticas struct {
	MaxEnVuelo  map[string]int `json:"max_en_vuelo"`
	Violaciones int            `json:"violaciones"`
	Requests    int            `json:"requests"`
}

func nuevaGuardia(limite int) *guardia {
	return &guardia{limite: limite, enVuelo: map[string]int{}, maxEnVuelo: map[string]int{}}
}

func (g *guardia) entrar(proveedor string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests++
	if g.enVuelo[proveedor] >= g.limite {
		g.violaciones++
		return false
	}
	g.enVuelo[proveedor]++
	if g.enVuelo[proveedor] > g.maxEnVuelo[proveedor] {
		g.maxEnVuelo[proveedor] = g.enVuelo[proveedor]
	}
	return true
}

func (g *guardia) salir(proveedor string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.enVuelo[proveedor]--
}

func (g *guardia) stats() estadisticas {
	g.mu.Lock()
	defer g.mu.Unlock()
	copia := make(map[string]int, len(g.maxEnVuelo))
	for k, v := range g.maxEnVuelo {
		copia[k] = v
	}
	return estadisticas{MaxEnVuelo: copia, Violaciones: g.violaciones, Requests: g.requests}
}
