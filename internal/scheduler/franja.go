package scheduler

import (
	"context"
	"fmt"
	"log/slog"

	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/fuente"
)

type Scheduler struct {
	Fuente fuente.Fuente
	Cola   cola.Cola
}

// EncolarFranja publica los registros de la franja. Es idempotente por
// (fecha, slot) gracias a corridas_diarias, y respeta la política de
// medianoche: un proveedor con pendientes del día anterior no se encola.
func (s *Scheduler) EncolarFranja(ctx context.Context, fecha string, slot int) (int, error) {
	nueva, err := s.Fuente.IniciarFranja(ctx, fecha, slot)
	if err != nil {
		return 0, err
	}
	if !nueva {
		slog.Info("franja ya publicada", "fecha", fecha, "slot", slot)
		return 0, nil
	}

	atrasados, err := s.Fuente.ProveedoresAtrasados(ctx)
	if err != nil {
		return 0, err
	}
	saltar := make(map[string]bool, len(atrasados))
	for _, p := range atrasados {
		saltar[p] = true
	}

	regs, err := s.Fuente.RegistrosDeSlot(ctx, slot)
	if err != nil {
		return 0, err
	}
	msgs := make([]dominio.Mensaje, 0, len(regs))
	for _, r := range regs {
		if saltar[r.Proveedor] {
			continue
		}
		msgs = append(msgs, dominio.Mensaje{
			RegistroID: r.ID, Proveedor: r.Proveedor, Endpoint: r.Endpoint, Body: r.Body, Fecha: fecha,
		})
	}
	if len(msgs) > 0 {
		if err := s.Cola.Enviar(ctx, msgs); err != nil {
			return 0, fmt.Errorf("encolar franja %s/%d: %w", fecha, slot, err)
		}
	}
	if err := s.Fuente.CerrarFranja(ctx, fecha, slot, len(msgs)); err != nil {
		return len(msgs), err
	}
	slog.Info("franja encolada", "fecha", fecha, "slot", slot, "registros", len(regs), "encolados", len(msgs), "saltados", len(regs)-len(msgs))
	return len(msgs), nil
}
