package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"consultas-diarias/internal/dominio"
)

// Correr lanza `concurrencia` goroutines independientes. Cada una pide un
// único mensaje por ReceiveMessage y lo procesa hasta el final antes de
// volver a pedir. La serialización por proveedor la da SQS FIFO: mientras el
// mensaje está en vuelo ningún otro consumidor recibe su grupo. Pedir de a uno
// es necesario porque SQS sí devuelve varios mensajes del mismo grupo en un
// mismo receive, y procesarlos en paralelo violaría la invariante.
//
// Al cancelar el contexto cada goroutine deja de recibir, termina el mensaje
// que tenga en vuelo y sale; Correr vuelve cuando terminaron todas.
func Correr(ctx context.Context, p *Procesador, concurrencia int) {
	if concurrencia < 1 {
		concurrencia = 1
	}
	var wg sync.WaitGroup
	for range concurrencia {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumir(ctx, p)
		}()
	}
	wg.Wait()
}

// consumir recibe y procesa de a un mensaje hasta que se cancele el contexto.
func consumir(ctx context.Context, p *Procesador) {
	for ctx.Err() == nil {
		msgs, err := p.Cola.Recibir(ctx, 1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("recibir", "err", err)
			dormir(ctx, time.Second)
			continue
		}
		if len(msgs) == 0 {
			// El fake no bloquea; contra SQS el long polling ya esperó 20 s.
			dormir(ctx, 10*time.Millisecond)
			continue
		}
		for _, m := range msgs { // a lo sumo uno: se pidió max 1
			procesarUno(p, m)
		}
	}
}

// procesarUno usa un contexto propio: una cancelación por apagado no debe
// dejar un mensaje a medias. 45 s cubre el timeout HTTP de 30 s y queda por
// debajo del visibility timeout de 60 s.
func procesarUno(p *Procesador, m dominio.MensajeRecibido) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := p.Procesar(ctx, m); err != nil {
		slog.Error("procesar", "registro", m.RegistroID, "err", err)
	}
}

// dormir espera d o hasta que se cancele el contexto, lo que ocurra primero.
func dormir(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
