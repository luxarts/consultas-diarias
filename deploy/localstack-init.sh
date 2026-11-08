#!/bin/bash
set -euo pipefail

awslocal sqs create-queue --queue-name consultas-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/consultas-dlq.fifo \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue --queue-name consultas.fifo --attributes "{
  \"FifoQueue\": \"true\",
  \"ContentBasedDeduplication\": \"false\",
  \"VisibilityTimeout\": \"60\",
  \"ReceiveMessageWaitTimeSeconds\": \"20\",
  \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"
}"

awslocal dynamodb create-table --table-name resultados \
  --attribute-definitions \
    AttributeName=record_id,AttributeType=N \
    AttributeName=fecha,AttributeType=S \
    AttributeName=proveedor,AttributeType=S \
    AttributeName=fecha_estado,AttributeType=S \
  --key-schema AttributeName=record_id,KeyType=HASH AttributeName=fecha,KeyType=RANGE \
  --global-secondary-indexes '[{
    "IndexName": "proveedor-fecha-index",
    "KeySchema": [{"AttributeName":"proveedor","KeyType":"HASH"},{"AttributeName":"fecha_estado","KeyType":"RANGE"}],
    "Projection": {"ProjectionType":"ALL"}
  }]' \
  --billing-mode PAY_PER_REQUEST

awslocal dynamodb update-time-to-live --table-name resultados \
  --time-to-live-specification Enabled=true,AttributeName=ttl

echo "localstack listo"
