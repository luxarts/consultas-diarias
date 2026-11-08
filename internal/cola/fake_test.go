package cola

import (
	"context"
	"testing"

	"consultas-diarias/internal/dominio"
)

func ids(ms []dominio.MensajeRecibido) []int64 {
	out := make([]int64, len(ms))
	for i, m := range ms {
		out[i] = m.RegistroID
	}
	return out
}

func iguales(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Como SQS FIFO, un receive con max > 1 puede traer varios mensajes del mismo
// grupo, en orden; el bloqueo de grupo sólo excluye a otros receives.
func TestFakeLoteTraeVariosDelMismoProveedor(t *testing.T) {
	f := NuevaFake()
	ctx := context.Background()
	_ = f.Enviar(ctx, []dominio.Mensaje{
		{RegistroID: 1, Proveedor: "a"},
		{RegistroID: 2, Proveedor: "b"},
		{RegistroID: 3, Proveedor: "a"},
	})
	got, _ := f.Recibir(ctx, 10)
	if !iguales(ids(got), []int64{1, 3, 2}) {
		t.Fatalf("lote = %v, quiero [1 3 2]: primero todo el grupo a, después b", ids(got))
	}
	if otra, _ := f.Recibir(ctx, 10); len(otra) != 0 {
		t.Fatalf("con todo en vuelo no debía entregar nada: %v", ids(otra))
	}
}

// Mientras quede un mensaje del grupo en vuelo, el grupo sigue bloqueado.
func TestFakeGrupoBloqueadoMientrasQuedeUnoEnVuelo(t *testing.T) {
	f := NuevaFake()
	ctx := context.Background()
	_ = f.Enviar(ctx, []dominio.Mensaje{{RegistroID: 1, Proveedor: "a"}, {RegistroID: 2, Proveedor: "a"}})
	got, _ := f.Recibir(ctx, 2)
	_ = f.Enviar(ctx, []dominio.Mensaje{{RegistroID: 3, Proveedor: "a"}})
	_ = f.Borrar(ctx, got[0].Recibo)
	if otra, _ := f.Recibir(ctx, 10); len(otra) != 0 {
		t.Fatalf("con el 2 en vuelo no debía entregar el 3: %v", ids(otra))
	}
	_ = f.Borrar(ctx, got[1].Recibo)
	if otra, _ := f.Recibir(ctx, 10); !iguales(ids(otra), []int64{3}) {
		t.Fatalf("tras borrar ambos debía entregar el 3: %v", ids(otra))
	}
}

// Con max = 1 (lo que pide el worker) la entrega por grupo es serial.
func TestFakeDeAUnoNoEntregaDosDelMismoProveedor(t *testing.T) {
	f := NuevaFake()
	ctx := context.Background()
	_ = f.Enviar(ctx, []dominio.Mensaje{
		{RegistroID: 1, Proveedor: "a"},
		{RegistroID: 2, Proveedor: "a"},
		{RegistroID: 3, Proveedor: "b"},
	})
	primero, _ := f.Recibir(ctx, 1)
	segundo, _ := f.Recibir(ctx, 1)
	if !iguales(ids(primero), []int64{1}) || !iguales(ids(segundo), []int64{3}) {
		t.Fatalf("entregas = %v %v, quiero [1] [3]", ids(primero), ids(segundo))
	}
	if otra, _ := f.Recibir(ctx, 1); len(otra) != 0 {
		t.Fatalf("no debía entregar nada con 'a' y 'b' en vuelo: %v", ids(otra))
	}
	_ = f.Borrar(ctx, primero[0].Recibo)
	if tercera, _ := f.Recibir(ctx, 1); !iguales(ids(tercera), []int64{2}) {
		t.Fatalf("tras borrar debía entregar el 2: %v", ids(tercera))
	}
}

func TestFakeCambiarVisibilidadIncrementaIntento(t *testing.T) {
	f := NuevaFake()
	ctx := context.Background()
	_ = f.Enviar(ctx, []dominio.Mensaje{{RegistroID: 1, Proveedor: "a"}})
	got, _ := f.Recibir(ctx, 1)
	_ = f.CambiarVisibilidad(ctx, got[0].Recibo, 0)
	again, _ := f.Recibir(ctx, 1)
	if again[0].Intento != 2 {
		t.Fatalf("intento = %d, quiero 2", again[0].Intento)
	}
	if len(f.Visibilidades) != 1 || f.Visibilidades[0] != 0 {
		t.Fatalf("visibilidades = %v", f.Visibilidades)
	}
}
