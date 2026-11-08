package resultados

import (
	"context"
	"fmt"
	"sync"

	"consultas-diarias/internal/dominio"
)

type Fake struct {
	mu      sync.Mutex
	Items   map[string]dominio.Resultado // "id:fecha"
	FallarN int                          // próximas N escrituras fallan con error genérico
}

func NuevaFake() *Fake { return &Fake{Items: map[string]dominio.Resultado{}} }

func clave(r dominio.Resultado) string { return fmt.Sprintf("%d:%s", r.RegistroID, r.Fecha) }

func (f *Fake) Guardar(_ context.Context, r dominio.Resultado) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FallarN > 0 {
		f.FallarN--
		return fmt.Errorf("fallo simulado")
	}
	if _, ok := f.Items[clave(r)]; ok {
		return ErrYaExiste
	}
	f.Items[clave(r)] = r
	return nil
}
