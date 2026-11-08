package fuente

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/dominio"
)

type Postgres struct{ pool *pgxpool.Pool }

func NuevaPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (p *Postgres) RegistrosDeSlot(ctx context.Context, slot int) ([]dominio.Registro, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, proveedor, endpoint, body, slot FROM registros WHERE slot = $1 AND activo ORDER BY proveedor, id`, slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dominio.Registro
	for rows.Next() {
		var r dominio.Registro
		if err := rows.Scan(&r.ID, &r.Proveedor, &r.Endpoint, &r.Body, &r.Slot); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Postgres) IniciarFranja(ctx context.Context, fecha string, slot int) (bool, error) {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO corridas_diarias (fecha, slot, estado) VALUES ($1, $2, 'encolando')`, fecha, slot)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("iniciar franja: %w", err)
	}
	return true, nil
}

func (p *Postgres) CerrarFranja(ctx context.Context, fecha string, slot int, encolados int) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE corridas_diarias SET estado = 'encolada', encolados = $3, finalizada_en = now() WHERE fecha = $1 AND slot = $2`,
		fecha, slot, encolados)
	return err
}

func (p *Postgres) ProveedoresAtrasados(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx, `SELECT proveedor FROM proveedores_estado WHERE pendientes_ayer > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
