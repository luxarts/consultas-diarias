package cola

import (
	"context"
	"fmt"
	"sync"
	"time"

	"consultas-diarias/internal/dominio"
)

// Fake es una cola en memoria que imita la semántica de grupos de SQS FIFO:
// un receive puede devolver varios mensajes consecutivos del mismo proveedor
// (hasta max), y mientras alguno de un proveedor está en vuelo ningún otro
// receive entrega mensajes de ese proveedor.
type Fake struct {
	mu            sync.Mutex
	pendiente     []dominio.MensajeRecibido
	enVuelo       map[string]dominio.MensajeRecibido // recibo -> mensaje
	seq           int
	Borrados      []int64         // registro_id de los mensajes borrados, en orden
	Visibilidades []time.Duration // esperas pedidas en CambiarVisibilidad, en orden
	Enviados      int
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

// Recibir recorre los proveedores en el orden de su primer mensaje pendiente
// y, para cada uno que no tenga mensajes en vuelo, entrega sus mensajes en
// orden hasta completar max. Como SQS, intenta llenar el lote con el mismo
// grupo antes de pasar a otro.
func (f *Fake) Recibir(_ context.Context, max int) ([]dominio.MensajeRecibido, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bloqueados := map[string]bool{}
	for _, m := range f.enVuelo {
		bloqueados[m.Proveedor] = true
	}
	var orden []string
	porProveedor := map[string][]int{} // proveedor -> índices en pendiente
	for i, m := range f.pendiente {
		if _, ok := porProveedor[m.Proveedor]; !ok {
			orden = append(orden, m.Proveedor)
		}
		porProveedor[m.Proveedor] = append(porProveedor[m.Proveedor], i)
	}
	entregado := map[int]bool{}
	var salida []dominio.MensajeRecibido
	for _, prov := range orden {
		if bloqueados[prov] {
			continue
		}
		for _, i := range porProveedor[prov] {
			if len(salida) >= max {
				break
			}
			m := f.pendiente[i]
			f.seq++
			m.Recibo = fmt.Sprintf("r%d", f.seq)
			f.enVuelo[m.Recibo] = m
			entregado[i] = true
			salida = append(salida, m)
		}
	}
	resto := f.pendiente[:0:0]
	for i, m := range f.pendiente {
		if !entregado[i] {
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

// CambiarVisibilidad devuelve el mensaje a la cabeza de la cola de inmediato,
// sin esperar: el fake no modela el paso del tiempo. La espera pedida queda
// registrada en Visibilidades.
func (f *Fake) CambiarVisibilidad(_ context.Context, recibo string, espera time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.enVuelo[recibo]
	if !ok {
		return fmt.Errorf("recibo desconocido: %s", recibo)
	}
	delete(f.enVuelo, recibo)
	f.Visibilidades = append(f.Visibilidades, espera)
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

// EnVuelo devuelve cuántos mensajes están entregados y sin borrar.
func (f *Fake) EnVuelo() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.enVuelo)
}
