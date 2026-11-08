package cola

import (
	"context"
	"testing"

	"consultas-diarias/internal/dominio"
)

func TestFakeNoEntregaDosDelMismoProveedor(t *testing.T) {
	f := NuevaFake()
	ctx := context.Background()
	_ = f.Enviar(ctx, []dominio.Mensaje{
		{RegistroID: 1, Proveedor: "a"},
		{RegistroID: 2, Proveedor: "a"},
		{RegistroID: 3, Proveedor: "b"},
	})
	got, _ := f.Recibir(ctx, 10)
	if len(got) != 2 || got[0].RegistroID != 1 || got[1].RegistroID != 3 {
		t.Fatalf("primera entrega inesperada: %+v", got)
	}
	otra, _ := f.Recibir(ctx, 10)
	if len(otra) != 0 {
		t.Fatalf("no debía entregar nada con 'a' en vuelo: %+v", otra)
	}
	_ = f.Borrar(ctx, got[0].Recibo)
	tercera, _ := f.Recibir(ctx, 10)
	if len(tercera) != 1 || tercera[0].RegistroID != 2 {
		t.Fatalf("tras borrar debía entregar el 2: %+v", tercera)
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
}
