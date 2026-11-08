package cola

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"consultas-diarias/internal/dominio"
)

// SQS implementa Cola sobre una cola FIFO. El MessageGroupId es el
// proveedor, lo que delega en SQS la serialización por proveedor: mientras un
// mensaje de un grupo está en vuelo, ningún otro consumidor recibe mensajes de
// ese grupo. Ojo: un mismo ReceiveMessage con max > 1 sí puede devolver varios
// mensajes consecutivos del mismo grupo; por eso el worker pide de a uno.
type SQS struct {
	cliente *sqs.Client
	url     string
	// Espera es el long polling de Recibir. 20 s en producción; las pruebas
	// la acortan para que apagar un worker no demore tanto.
	Espera time.Duration
}

func NuevaSQS(cliente *sqs.Client, url string) *SQS {
	return &SQS{cliente: cliente, url: url, Espera: 20 * time.Second}
}

func (s *SQS) Enviar(ctx context.Context, msgs []dominio.Mensaje) error {
	for i := 0; i < len(msgs); i += 10 {
		fin := min(i+10, len(msgs))
		entradas := make([]types.SendMessageBatchRequestEntry, 0, fin-i)
		for j, m := range msgs[i:fin] {
			cuerpo, err := json.Marshal(m)
			if err != nil {
				return err
			}
			entradas = append(entradas, types.SendMessageBatchRequestEntry{
				Id:                     aws.String(strconv.Itoa(j)),
				MessageBody:            aws.String(string(cuerpo)),
				MessageGroupId:         aws.String(m.Proveedor),
				MessageDeduplicationId: aws.String(fmt.Sprintf("%d:%s", m.RegistroID, m.Fecha)),
			})
		}
		out, err := s.cliente.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(s.url),
			Entries:  entradas,
		})
		if err != nil {
			return fmt.Errorf("SendMessageBatch: %w", err)
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("SendMessageBatch: %d entradas fallidas, primera: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
		}
	}
	return nil
}

func (s *SQS) Recibir(ctx context.Context, max int) ([]dominio.MensajeRecibido, error) {
	espera := int32(s.Espera / time.Second)
	if espera < 0 {
		espera = 0
	}
	if espera > 20 {
		espera = 20
	}
	out, err := s.cliente.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(s.url),
		MaxNumberOfMessages:         int32(min(max, 10)),
		WaitTimeSeconds:             espera,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		return nil, fmt.Errorf("ReceiveMessage: %w", err)
	}
	var msgs []dominio.MensajeRecibido
	for _, m := range out.Messages {
		var mr dominio.MensajeRecibido
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &mr.Mensaje); err != nil {
			return nil, fmt.Errorf("mensaje malformado %s: %w", aws.ToString(m.MessageId), err)
		}
		mr.Recibo = aws.ToString(m.ReceiptHandle)
		mr.Intento, _ = strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
		if mr.Intento == 0 {
			mr.Intento = 1
		}
		msgs = append(msgs, mr)
	}
	return msgs, nil
}

func (s *SQS) Borrar(ctx context.Context, recibo string) error {
	_, err := s.cliente.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.url),
		ReceiptHandle: aws.String(recibo),
	})
	return err
}

func (s *SQS) CambiarVisibilidad(ctx context.Context, recibo string, espera time.Duration) error {
	// Una espera de 0 devuelve el mensaje a la cola de inmediato (la usa el
	// worker al agotar intentos para que SQS lo mueva ya a la DLQ). Una espera
	// positiva menor a un segundo, como un backoff escalado, se redondea a 1 s.
	var segundos int32
	if espera > 0 {
		segundos = int32(min(max(espera/time.Second, 1), 43200))
	}
	_, err := s.cliente.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.url),
		ReceiptHandle:     aws.String(recibo),
		VisibilityTimeout: segundos,
	})
	return err
}
