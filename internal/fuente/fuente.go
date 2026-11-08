// Package fuente lee la tabla de registros y lleva el estado de las franjas.
package fuente

import (
	"context"

	"consultas-diarias/internal/dominio"
)

type Fuente interface {
	// RegistrosDeSlot devuelve los registros activos de la franja.
	RegistrosDeSlot(ctx context.Context, slot int) ([]dominio.Registro, error)
	// IniciarFranja inserta (fecha, slot) en corridas_diarias. Devuelve
	// false si ya existía: otra ejecución la tomó.
	IniciarFranja(ctx context.Context, fecha string, slot int) (bool, error)
	CerrarFranja(ctx context.Context, fecha string, slot int, encolados int) error
	// ProveedoresAtrasados devuelve los proveedores con pendientes del día
	// anterior, que no deben encolarse hoy.
	ProveedoresAtrasados(ctx context.Context) ([]string, error)
}
