package resultados

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"consultas-diarias/internal/dominio"
)

type Dynamo struct {
	cliente *dynamodb.Client
	tabla   string
}

func NuevaDynamo(cliente *dynamodb.Client, tabla string) *Dynamo {
	return &Dynamo{cliente: cliente, tabla: tabla}
}

type item struct {
	RecordID    int64  `dynamodbav:"record_id"`
	Fecha       string `dynamodbav:"fecha"`
	Proveedor   string `dynamodbav:"proveedor"`
	FechaEstado string `dynamodbav:"fecha_estado"`
	Estado      string `dynamodbav:"estado"`
	HTTPStatus  int    `dynamodbav:"http_status"`
	Intentos    int    `dynamodbav:"intentos"`
	Respuesta   string `dynamodbav:"respuesta,omitempty"`
	Error       string `dynamodbav:"error,omitempty"`
	LatenciaMs  int64  `dynamodbav:"latencia_ms"`
	ProcesadoEn string `dynamodbav:"procesado_en"`
	TTL         int64  `dynamodbav:"ttl"`
}

// Guardar es un PutItem condicional: si ya hay un item con estado para
// (record_id, fecha) devuelve ErrYaExiste y no toca nada.
func (d *Dynamo) Guardar(ctx context.Context, r dominio.Resultado) error {
	it := item{
		RecordID:    r.RegistroID,
		Fecha:       r.Fecha,
		Proveedor:   r.Proveedor,
		FechaEstado: r.Fecha + "#" + r.Estado,
		Estado:      r.Estado,
		HTTPStatus:  r.HTTPStatus,
		Intentos:    r.Intentos,
		Respuesta:   string(r.Respuesta),
		Error:       r.Error,
		LatenciaMs:  r.LatenciaMs,
		ProcesadoEn: r.ProcesadoEn.UTC().Format(time.RFC3339),
		TTL:         r.ProcesadoEn.AddDate(0, 0, dominio.RetencionDias).Unix(),
	}
	av, err := attributevalue.MarshalMap(it)
	if err != nil {
		return err
	}
	_, err = d.cliente.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(d.tabla),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(estado)"),
	})
	var cond *types.ConditionalCheckFailedException
	if errors.As(err, &cond) {
		return ErrYaExiste
	}
	if err != nil {
		return fmt.Errorf("PutItem: %w", err)
	}
	return nil
}

// Leer existe para los tests de integración.
func (d *Dynamo) Leer(ctx context.Context, id int64, fecha string) (dominio.Resultado, bool, error) {
	out, err := d.cliente.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tabla),
		Key: map[string]types.AttributeValue{
			"record_id": &types.AttributeValueMemberN{Value: fmt.Sprint(id)},
			"fecha":     &types.AttributeValueMemberS{Value: fecha},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil || out.Item == nil {
		return dominio.Resultado{}, false, err
	}
	var it item
	if err := attributevalue.UnmarshalMap(out.Item, &it); err != nil {
		return dominio.Resultado{}, false, err
	}
	return dominio.Resultado{
		RegistroID: it.RecordID, Fecha: it.Fecha, Proveedor: it.Proveedor, Estado: it.Estado,
		HTTPStatus: it.HTTPStatus, Intentos: it.Intentos, Error: it.Error, LatenciaMs: it.LatenciaMs,
	}, true, nil
}
