// Package awscfg carga la configuración del SDK de AWS desde el entorno.
package awscfg

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Cargar usa región, credenciales y AWS_ENDPOINT_URL del entorno. Contra
// LocalStack el endpoint apunta a http://localstack:4566; en AWS no se define.
func Cargar(ctx context.Context) (aws.Config, error) {
	return config.LoadDefaultConfig(ctx)
}
