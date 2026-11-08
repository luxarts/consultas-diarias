// seed carga un dataset sintético en la tabla registros y calcula los slots.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const sqlSlots = `
UPDATE registros r SET slot = s.slot
FROM (
  SELECT id,
         (ROW_NUMBER() OVER (PARTITION BY proveedor ORDER BY id) - 1) * 288
           / COUNT(*) OVER (PARTITION BY proveedor) AS slot
  FROM registros
  WHERE activo
) s
WHERE r.id = s.id`

func entero(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	total := entero("REGISTROS", 100000)
	proveedores := entero("PROVEEDORES", 200)

	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `TRUNCATE registros, corridas_diarias, proveedores_estado`); err != nil {
		slog.Error("truncate", "err", err)
		os.Exit(1)
	}

	reparto := repartoPareto(total, proveedores, 1.1)
	filas := make([][]any, 0, total)
	for i, n := range reparto {
		prov := fmt.Sprintf("proveedor-%03d", i)
		for j := 0; j < n; j++ {
			filas = append(filas, []any{prov, "/consulta", fmt.Sprintf(`{"cuenta":"%s-%d"}`, prov, j)})
		}
	}

	inicio := time.Now()
	n, err := pool.CopyFrom(ctx, pgx.Identifier{"registros"}, []string{"proveedor", "endpoint", "body"}, pgx.CopyFromRows(filas))
	if err != nil {
		slog.Error("copy", "err", err)
		os.Exit(1)
	}
	slog.Info("registros cargados", "n", n, "en", time.Since(inicio))

	inicio = time.Now()
	if _, err := pool.Exec(ctx, sqlSlots); err != nil {
		slog.Error("slots", "err", err)
		os.Exit(1)
	}
	slog.Info("slots asignados", "en", time.Since(inicio))

	var maxProv string
	var maxN int
	_ = pool.QueryRow(ctx, `SELECT proveedor, count(*) FROM registros GROUP BY 1 ORDER BY 2 DESC LIMIT 1`).Scan(&maxProv, &maxN)
	slog.Info("proveedor más cargado", "proveedor", maxProv, "registros", maxN, "por_franja", maxN/288)
}
