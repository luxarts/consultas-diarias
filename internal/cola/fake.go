package cola

import (
	"context"
	"fmt"
	"sync"
	"time"

	"consultas-diarias/internal/dominio"
)

// Fake es una cola en memoria con semántica FIFO por proveedor.
type Fake struct {
	mu        sync.Mutex
	pendiente []dominio.MensajeRecibido
	enVuelo   map[string]dominio.MensajeRecibido // recibo -> mensaje
	seq       int
	Borrados  []int64 // registro_id de los mensajes borrados, en orden
	Enviados  int
}

func NuevaFake() *Fake {
	return &Fake{enVuelo: map[string]dominio.MensajeRecibido{}}
}

func (f *Fake) Enviar(_ context.Context, msgs []dominio.Mensaje) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range msgs {
		f.pendiente = append(f.pendiente, dominio.MensajeRecibido{Mensaje: m, Intento: 1})
		f.Enviados++
	}
	return nil
}

func (f *Fake) Recibir(_ context.Context, max int) ([]dominio.MensajeRecibido, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bloqueados := map[string]bool{}
	for _, m := range f.enVuelo {
		bloqueados[m.Proveedor] = true
	}
	var salida []dominio.MensajeRecibido
	var resto []dominio.MensajeRecibido
	for _, m := range f.pendiente {
		if len(salida) < max && !bloqueados[m.Proveedor] {
			f.seq++
			m.Recibo = fmt.Sprintf("r%d", f.seq)
			f.enVuelo[m.Recibo] = m
			bloqueados[m.Proveedor] = true
			salida = append(salida, m)
		} else {
			resto = append(resto, m)
		}
	}
	f.pendiente = resto
	return salida, nil
}

func (f *Fake) Borrar(_ context.Context, recibo string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.enVuelo[recibo]
	if !ok {
		return fmt.Errorf("recibo desconocido: %s", recibo)
	}
	delete(f.enVuelo, recibo)
	f.Borrados = append(f.Borrados, m.RegistroID)
	return nil
}

func (f *Fake) CambiarVisibilidad(_ context.Context, recibo string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.enVuelo[recibo]
	if !ok {
		return fmt.Errorf("recibo desconocido: %s", recibo)
	}
	delete(f.enVuelo, recibo)
	m.Recibo = ""
	m.Intento++
	f.pendiente = append([]dominio.MensajeRecibido{m}, f.pendiente...)
	return nil
}

// Pendientes devuelve cuántos mensajes quedan sin entregar.
func (f *Fake) Pendientes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pendiente)
}
