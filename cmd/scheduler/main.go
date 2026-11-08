// scheduler publica en la cola los registros de la franja actual.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/awscfg"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/fuente"
	"consultas-diarias/internal/scheduler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		slog.Error("config aws", "err", err)
		os.Exit(1)
	}
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	escala, _ := strconv.ParseFloat(os.Getenv("ESCALA_TIEMPO"), 64)
	if escala < 1 {
		escala = 1
	}
	s := &scheduler.Scheduler{
		Fuente: fuente.NuevaPostgres(pool),
		Cola:   cola.NuevaSQS(sqs.NewFromConfig(cfg), os.Getenv("COLA_URL")),
	}

	duracionFranja := time.Duration(float64(24*time.Hour/dominio.FranjasPorDia) / escala)
	inicio := time.Now()
	slog.Info("scheduler iniciado", "escala", escala, "franja", duracionFranja)

	// Encola la franja actual de inmediato y luego una por tick. Si una
	// franja se saltea (proceso caído), la siguiente ejecución la detecta
	// porque SlotActual avanzó y la fila de corridas_diarias no existe:
	// recorremos desde la última publicada hasta la actual.
	ultimo := -1
	tick := time.NewTicker(duracionFranja)
	defer tick.Stop()
	for {
		slot, fecha := scheduler.SlotActual(time.Now(), inicio, escala)
		if slot != ultimo {
			if _, err := s.EncolarFranja(ctx, fecha, slot); err != nil {
				slog.Error("encolar", "slot", slot, "err", err)
			}
			ultimo = slot
		}
		select {
		case <-ctx.Done():
			slog.Info("scheduler detenido")
			return
		case <-tick.C:
		}
	}
}
