package main

import (
	"strings"
	"sync"
)

// guardia cuenta requests en vuelo por proveedor y registra el máximo
// observado. Es la aserción empírica de "no concurrencia por proveedor".
type guardia struct {
	mu            sync.Mutex
	limite        int
	enVuelo       map[string]int
	maxEnVuelo    map[string]int
	enVueloTotal  int
	maxEnVueloTot int
	violaciones   int
	requests      int
	secuencias    map[string][]string
}

// estadisticas lleva los escalares primero para que se lean de un vistazo
// aunque el mapa por proveedor sea largo.
type estadisticas struct {
	Violaciones      int            `json:"violaciones"`
	Requests         int            `json:"requests"`
	MaxEnVueloGlobal int            `json:"max_en_vuelo_global"` // todos los proveedores juntos
	MaxEnVuelo       map[string]int `json:"max_en_vuelo"`
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
	g.enVueloTotal++
	g.maxEnVueloTot = max(g.maxEnVueloTot, g.enVueloTotal)
	return true
}

func (g *guardia) salir(proveedor string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.enVuelo[proveedor]--
	g.enVueloTotal--
}

// siguienteForzado devuelve el próximo código de la secuencia del proveedor.
// La primera llamada fija la secuencia; al agotarse devuelve false.
func (g *guardia) siguienteForzado(proveedor, secuencia string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.secuencias == nil {
		g.secuencias = map[string][]string{}
	}
	if _, ok := g.secuencias[proveedor]; !ok {
		g.secuencias[proveedor] = strings.Split(secuencia, ",")
	}
	cola := g.secuencias[proveedor]
	if len(cola) == 0 {
		return "", false
	}
	g.secuencias[proveedor] = cola[1:]
	return strings.TrimSpace(cola[0]), true
}

func (g *guardia) stats() estadisticas {
	g.mu.Lock()
	defer g.mu.Unlock()
	copia := make(map[string]int, len(g.maxEnVuelo))
	for k, v := range g.maxEnVuelo {
		copia[k] = v
	}
	return estadisticas{Violaciones: g.violaciones, Requests: g.requests, MaxEnVueloGlobal: g.maxEnVueloTot, MaxEnVuelo: copia}
}
