// api-mock simula la API interna (API Gateway + dos Lambdas): latencia
// configurable, errores con probabilidad configurable y una guardia que
// detecta si alguien la llama en paralelo para el mismo proveedor.
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	puerto       string
	latenciaBase time.Duration
	latenciaVar  time.Duration
	prob500      float64
	prob503      float64
	prob429      float64
	probTimeout  float64
	prob400      float64
	limite       int
}

func leerConfig() config {
	f := func(k string, def float64) float64 {
		if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
			return v
		}
		return def
	}
	ms := func(k string, def int) time.Duration {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
			return time.Duration(v) * time.Millisecond
		}
		return time.Duration(def) * time.Millisecond
	}
	puerto := os.Getenv("PUERTO")
	if puerto == "" {
		puerto = "8080"
	}
	return config{
		puerto:       puerto,
		latenciaBase: ms("LATENCIA_BASE_MS", 200),
		latenciaVar:  ms("LATENCIA_VAR_MS", 100),
		prob500:      f("PROB_500", 0),
		prob503:      f("PROB_503", 0),
		prob429:      f("PROB_429", 0),
		probTimeout:  f("PROB_TIMEOUT", 0),
		prob400:      f("PROB_400", 0),
		limite:       int(f("MAX_CONCURRENCIA_PROVEEDOR", 1)),
	}
}

func main() {
	cfg := leerConfig()
	g := nuevaGuardia(cfg.limite)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(g.stats())
	})

	mux.HandleFunc("POST /{proveedor}/{resto...}", func(w http.ResponseWriter, r *http.Request) {
		proveedor := r.PathValue("proveedor")
		if !g.entrar(proveedor) {
			slog.Error("violación de concurrencia", "proveedor", proveedor)
			http.Error(w, `{"error":"concurrencia por proveedor"}`, http.StatusConflict)
			return
		}
		defer g.salir(proveedor)

		// Simula las dos Lambdas: dos tramos de latencia.
		time.Sleep(latencia(cfg) / 2)
		time.Sleep(latencia(cfg) / 2)

		// X-Forzar-Secuencia responde una secuencia de códigos por proveedor.
		if seq := r.Header.Get("X-Forzar-Secuencia"); seq != "" {
			if code, ok := g.siguienteForzado(proveedor, seq); ok {
				responder(w, code, cfg)
				return
			}
		}

		// Cabecera X-Forzar permite a los tests pedir un código concreto.
		if forzado := r.Header.Get("X-Forzar"); forzado != "" {
			responder(w, forzado, cfg)
			return
		}
		responder(w, sortear(cfg), cfg)
	})

	slog.Info("api-mock escuchando", "puerto", cfg.puerto, "limite", cfg.limite)
	if err := http.ListenAndServe(":"+cfg.puerto, mux); err != nil {
		slog.Error("servidor", "err", err)
		os.Exit(1)
	}
}

func latencia(cfg config) time.Duration {
	if cfg.latenciaVar == 0 {
		return cfg.latenciaBase
	}
	return cfg.latenciaBase + time.Duration(rand.Int64N(int64(cfg.latenciaVar)))
}

func sortear(cfg config) string {
	x := rand.Float64()
	acum := 0.0
	for _, c := range []struct {
		p    float64
		code string
	}{{cfg.prob500, "500"}, {cfg.prob503, "503"}, {cfg.prob429, "429"}, {cfg.probTimeout, "timeout"}, {cfg.prob400, "400"}} {
		acum += c.p
		if x < acum {
			return c.code
		}
	}
	return "200"
}

func responder(w http.ResponseWriter, code string, _ config) {
	w.Header().Set("Content-Type", "application/json")
	switch strings.ToLower(code) {
	case "timeout":
		time.Sleep(35 * time.Second) // mayor al timeout del cliente (30 s)
		w.WriteHeader(http.StatusGatewayTimeout)
	case "200":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"ts":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`))
	default:
		n, err := strconv.Atoi(code)
		if err != nil {
			n = 500
		}
		w.WriteHeader(n)
		_, _ = w.Write([]byte(`{"error":"simulado"}`))
	}
}
