// Package resultados persiste el resultado de cada consulta diaria.
package resultados

import (
	"context"
	"errors"

	"consultas-diarias/internal/dominio"
)

// ErrYaExiste indica que ya hay un resultado con estado final para ese
// registro y fecha. El llamador debe tratarlo como éxito idempotente.
var ErrYaExiste = errors.New("resultado ya registrado")

type Resultados interface {
	Guardar(ctx context.Context, r dominio.Resultado) error // ErrYaExiste si hay estado final
}
