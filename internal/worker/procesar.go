package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

// Procesador ejecuta una consulta y decide el destino del mensaje.
type Procesador struct {
	Cola       cola.Cola
	Resultados resultados.Resultados
	API        apiclient.ClienteAPI
	Ahora      func() time.Time
	Rnd        func() float64
	Escala     float64
}

// Procesar nunca deja un mensaje en limbo: lo borra, lo reprograma con
// backoff o, al agotar intentos, lo devuelve con visibilidad 0 para que SQS
// lo mueva a la DLQ en la siguiente recepción.
func (p *Procesador) Procesar(ctx context.Context, m dominio.MensajeRecibido) error {
	inicio := p.Ahora()
	ctxAPI, cancel := context.WithTimeout(ctx, dominio.TimeoutHTTP)
	defer cancel()
	status, body, err := p.API.Consultar(ctxAPI, m.Proveedor, m.Endpoint, m.Body)
	fin := p.Ahora()

	res := dominio.Resultado{
		RegistroID:  m.RegistroID,
		Fecha:       m.Fecha,
		Proveedor:   m.Proveedor,
		HTTPStatus:  status,
		Intentos:    m.Intento,
		LatenciaMs:  fin.Sub(inicio).Milliseconds(),
		ProcesadoEn: fin,
	}

	switch Clasificar(status, body, err) {
	case dominio.Exito:
		res.Estado = dominio.EstadoOK
		res.Respuesta = body
		return p.cerrar(ctx, m, res)

	case dominio.NoReintentable:
		res.Estado = dominio.EstadoFalloDefinitivo
		res.Error = describir(status, body, err)
		return p.cerrar(ctx, m, res)

	default: // Reintentable
		espera, reintentar := Backoff(m.Intento, p.Rnd)
		if !reintentar {
			res.Estado = dominio.EstadoAgotado
			res.Error = describir(status, body, err)
			if err := p.guardar(ctx, m, res); err != nil {
				return err
			}
			slog.Warn("agotado", "registro", m.RegistroID, "proveedor", m.Proveedor)
			// No se borra: con visibilidad 0 vuelve a la cola ya mismo y, como
			// superó maxReceiveCount, SQS lo mueve a la DLQ en vez de entregarlo.
			// Sin esto el proveedor quedaría bloqueado 60 s hasta que venza.
			return p.Cola.CambiarVisibilidad(ctx, m.Recibo, 0)
		}
		slog.Info("reintento", "registro", m.RegistroID, "intento", m.Intento, "espera", espera)
		return p.Cola.CambiarVisibilidad(ctx, m.Recibo, p.escalar(espera))
	}
}

func (p *Procesador) cerrar(ctx context.Context, m dominio.MensajeRecibido, res dominio.Resultado) error {
	if err := p.guardar(ctx, m, res); err != nil {
		return err
	}
	return p.Cola.Borrar(ctx, m.Recibo)
}

// guardar trata ErrYaExiste como éxito (entrega duplicada) y cualquier otro
// error como reintentable: reprograma el mensaje y devuelve el error.
func (p *Procesador) guardar(ctx context.Context, m dominio.MensajeRecibido, res dominio.Resultado) error {
	err := p.Resultados.Guardar(ctx, res)
	if err == nil || errors.Is(err, resultados.ErrYaExiste) {
		return nil
	}
	espera, _ := Backoff(m.Intento, p.Rnd)
	if espera == 0 { // intento 5: Backoff no da espera, se usa la del primer intento
		espera = 30 * time.Second
	}
	_ = p.Cola.CambiarVisibilidad(ctx, m.Recibo, p.escalar(espera))
	return fmt.Errorf("guardar resultado: %w", err)
}

func (p *Procesador) escalar(d time.Duration) time.Duration {
	if p.Escala <= 1 {
		return d
	}
	return time.Duration(float64(d) / p.Escala)
}

func describir(status int, body []byte, err error) string {
	if err != nil {
		return err.Error()
	}
	if len(body) > 512 {
		body = body[:512]
	}
	return fmt.Sprintf("HTTP %d: %s", status, body)
}
