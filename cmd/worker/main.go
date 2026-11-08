// worker consume la cola FIFO, llama a la API interna y guarda resultados.
package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/awscfg"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/metricas"
	"consultas-diarias/internal/resultados"
	"consultas-diarias/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		slog.Error("config aws", "err", err)
		os.Exit(1)
	}
	concurrencia, _ := strconv.Atoi(os.Getenv("CONCURRENCIA"))
	if concurrencia <= 0 {
		concurrencia = 10
	}
	escala, _ := strconv.ParseFloat(os.Getenv("ESCALA_TIEMPO"), 64)
	if escala < 1 {
		escala = 1
	}

	// Las métricas se empujan al colector OTLP si el entorno lo define; el
	// worker no expone ningún puerto ni sabe cuántas réplicas hay.
	mets, exportador, err := metricas.Conectar(ctx, 5*time.Second)
	if err != nil {
		slog.Error("métricas", "err", err)
		os.Exit(1)
	}
	if exportador != nil {
		defer func() {
			ctxCierre, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := exportador.Cerrar(ctxCierre); err != nil {
				slog.Error("cerrar métricas", "err", err)
			}
		}()
	}

	p := &worker.Procesador{
		Cola:       cola.NuevaSQS(sqs.NewFromConfig(cfg), os.Getenv("COLA_URL")),
		Resultados: resultados.NuevaDynamo(dynamodb.NewFromConfig(cfg), os.Getenv("TABLA_RESULTADOS")),
		API:        apiclient.NuevoHTTP(os.Getenv("API_BASE_URL"), dominio.TimeoutHTTP),
		Ahora:      time.Now,
		Rnd:        rand.Float64,
		Escala:     escala,
		Metricas:   mets,
	}
	slog.Info("worker iniciado", "concurrencia", concurrencia, "escala", escala, "metricas", exportador != nil)
	worker.Correr(ctx, p, concurrencia)
	slog.Info("worker detenido")
}
