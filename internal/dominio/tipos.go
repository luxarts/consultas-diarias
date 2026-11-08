// Package dominio define los tipos compartidos por scheduler y worker.
package dominio

import (
	"encoding/json"
	"time"
)

const (
	FranjasPorDia = 288
	IntentosMax   = 5
	TimeoutHTTP   = 30 * time.Second
	RetencionDias = 90
)

const (
	EstadoOK              = "ok"
	EstadoFalloDefinitivo = "fallo_definitivo"
	EstadoAgotado         = "agotado"
)

// Registro es una fila de la tabla fuente.
type Registro struct {
	ID        int64
	Proveedor string
	Endpoint  string
	Body      json.RawMessage
	Slot      int
}

// Mensaje es lo que el scheduler publica en la cola.
type Mensaje struct {
	RegistroID int64           `json:"registro_id"`
	Proveedor  string          `json:"proveedor"`
	Endpoint   string          `json:"endpoint"`
	Body       json.RawMessage `json:"body"`
	Fecha      string          `json:"fecha"` // YYYY-MM-DD
}

// MensajeRecibido agrega los metadatos que entrega la cola.
type MensajeRecibido struct {
	Mensaje
	Recibo  string // identificador para borrar o cambiar visibilidad
	Intento int    // 1 en la primera entrega
}

// Resultado es lo que se persiste por registro y día.
type Resultado struct {
	RegistroID  int64
	Fecha       string
	Proveedor   string
	Estado      string
	HTTPStatus  int
	Intentos    int
	Respuesta   json.RawMessage
	Error       string
	LatenciaMs  int64
	ProcesadoEn time.Time
}

// Clasificacion es el veredicto sobre una respuesta de la API.
type Clasificacion int

const (
	Exito Clasificacion = iota
	Reintentable
	NoReintentable
)

func (c Clasificacion) String() string {
	switch c {
	case Exito:
		return "exito"
	case Reintentable:
		return "reintentable"
	default:
		return "no_reintentable"
	}
}
