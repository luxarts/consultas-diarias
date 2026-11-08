// scheduler publica en la cola los registros de cada franja del día.
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

	// Al arrancar encola sólo la franja actual: las franjas que pasaron con el
	// proceso caído no se recuperan (quedan sin fila en corridas_diarias, que
	// en producción es la alarma "franja perdida"). Dentro del proceso sí hay
	// catch-up: si un tick se atrasa o EncolarFranja tarda más que una franja,
	// el siguiente tick publica todas las intermedias. Con escala > 1 el
	// proceso termina al publicar la última franja del día comprimido.
	ultimaFecha, ultimoSlot := "", -1
	tick := time.NewTicker(duracionFranja)
	defer tick.Stop()
	for {
		slot, fecha := scheduler.SlotActual(time.Now(), inicio, escala)
		for _, f := range scheduler.FranjasAPublicar(ultimaFecha, ultimoSlot, fecha, slot) {
			if ctx.Err() != nil {
				break
			}
			if _, err := s.EncolarFranja(ctx, f.Fecha, f.Slot); err != nil {
				slog.Error("encolar", "fecha", f.Fecha, "slot", f.Slot, "err", err)
			}
			ultimaFecha, ultimoSlot = f.Fecha, f.Slot
			if escala > 1 && f.Slot == dominio.FranjasPorDia-1 {
				slog.Info("día comprimido completo", "fecha", f.Fecha)
				return
			}
		}
		select {
		case <-ctx.Done():
			slog.Info("scheduler detenido")
			return
		case <-tick.C:
		}
	}
}
