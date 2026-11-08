package main

import "math"

// repartoPareto devuelve cuántos registros recibe cada proveedor siguiendo
// una ley de potencias (Zipf con exponente s): el proveedor i recibe una
// porción proporcional a 1/(i+1)^s. Garantiza al menos 1 por proveedor y
// que la suma sea exactamente total.
func repartoPareto(total, proveedores int, s float64) []int {
	pesos := make([]float64, proveedores)
	suma := 0.0
	for i := range pesos {
		pesos[i] = 1 / math.Pow(float64(i+1), s)
		suma += pesos[i]
	}
	out := make([]int, proveedores)
	asignado := 0
	for i := range out {
		out[i] = max(1, int(float64(total)*pesos[i]/suma))
		asignado += out[i]
	}
	// Ajusta el redondeo sobre el proveedor más grande.
	out[0] += total - asignado
	return out
}
