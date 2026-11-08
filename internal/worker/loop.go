package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Correr recibe mensajes y los procesa con hasta `concurrencia` goroutines.
// Un ciclo de Recibir trae mensajes de proveedores distintos (garantía FIFO),
// así que procesarlos en paralelo no viola la serialización por proveedor.
// Al cancelar el contexto deja de recibir y espera a los que están en vuelo.
func Correr(ctx context.Context, p *Procesador, concurrencia int) {
	sem := make(chan struct{}, concurrencia)
	var wg sync.WaitGroup
	for ctx.Err() == nil {
		msgs, err := p.Cola.Recibir(ctx, concurrencia)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("recibir", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if len(msgs) == 0 {
			// El fake no bloquea; contra SQS el long polling ya esperó 20 s.
			select {
			case <-ctx.Done():
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		for _, m := range msgs {
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				// El procesamiento usa un contexto propio: una cancelación por
				// apagado no debe dejar un mensaje a medias.
				ctxMsg, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				if err := p.Procesar(ctxMsg, m); err != nil {
					slog.Error("procesar", "registro", m.RegistroID, "err", err)
				}
			}()
		}
	}
	wg.Wait()
}
