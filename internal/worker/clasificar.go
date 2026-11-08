// Package worker contiene la lógica de procesamiento de un mensaje.
package worker

import (
	"encoding/json"

	"consultas-diarias/internal/dominio"
)

// Clasificar decide qué hacer con la respuesta de la API.
// Cualquier error de transporte es reintentable: no sabemos si la API lo vio.
// Un 2xx con body inválido no se reintenta: la API respondió y volver a
// preguntar no va a cambiar el body.
func Clasificar(status int, body []byte, err error) dominio.Clasificacion {
	if err != nil {
		return dominio.Reintentable
	}
	switch {
	case status >= 200 && status < 300:
		if !json.Valid(body) {
			return dominio.NoReintentable
		}
		return dominio.Exito
	case status == 429:
		return dominio.Reintentable
	case status >= 500:
		return dominio.Reintentable
	default:
		return dominio.NoReintentable
	}
}
