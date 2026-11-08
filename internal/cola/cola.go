// Package cola abstrae la cola FIFO por proveedor.
package cola

import (
	"context"
	"time"

	"consultas-diarias/internal/dominio"
)

// Cola garantiza que de un mismo proveedor hay a lo sumo un mensaje
// entregado y no borrado ni expirado a la vez.
type Cola interface {
	Enviar(ctx context.Context, msgs []dominio.Mensaje) error
	Recibir(ctx context.Context, max int) ([]dominio.MensajeRecibido, error)
	Borrar(ctx context.Context, recibo string) error
	CambiarVisibilidad(ctx context.Context, recibo string, espera time.Duration) error
}
