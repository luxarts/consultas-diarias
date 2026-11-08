package fuente

import (
	"context"
	"fmt"
	"sync"

	"consultas-diarias/internal/dominio"
)

type Fake struct {
	mu        sync.Mutex
	Registros []dominio.Registro
	Franjas   map[string]int // "fecha:slot" -> encolados (-1 = iniciada, sin cerrar)
	Atrasados []string
}

func NuevaFake(regs []dominio.Registro) *Fake {
	return &Fake{Registros: regs, Franjas: map[string]int{}}
}

func (f *Fake) RegistrosDeSlot(_ context.Context, slot int) ([]dominio.Registro, error) {
	var out []dominio.Registro
	for _, r := range f.Registros {
		if r.Slot == slot {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *Fake) IniciarFranja(_ context.Context, fecha string, slot int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := fmt.Sprintf("%s:%d", fecha, slot)
	if _, ok := f.Franjas[k]; ok {
		return false, nil
	}
	f.Franjas[k] = -1
	return true, nil
}

func (f *Fake) CerrarFranja(_ context.Context, fecha string, slot int, encolados int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Franjas[fmt.Sprintf("%s:%d", fecha, slot)] = encolados
	return nil
}

func (f *Fake) ProveedoresAtrasados(_ context.Context) ([]string, error) {
	return f.Atrasados, nil
}
