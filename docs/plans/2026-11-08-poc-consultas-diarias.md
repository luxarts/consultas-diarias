# Plan de implementación: POC de consultas diarias distribuidas

> Este plan se ejecuta tarea por tarea. Cada tarea termina con un entregable testeable y un commit. Los pasos usan casillas (`- [ ]`) para seguimiento.

**Objetivo:** Construir la prueba de concepto descripta en la spec: scheduler, cola FIFO, workers, API mock y persistencia, corriendo en Docker Compose con LocalStack, y demostrar empíricamente la serialización por proveedor, los reintentos y la distribución horaria.

**Arquitectura:** Un scheduler publica en SQS FIFO los registros de la franja actual con `MessageGroupId = proveedor`. Workers en Go consumen con long polling, llaman a la API mock, clasifican la respuesta y escriben en DynamoDB. La lógica de negocio depende de interfaces (`Cola`, `Fuente`, `Resultados`, `ClienteAPI`) y se testea con fakes; los adaptadores AWS se prueban por integración contra LocalStack.

**Stack:** Go 1.23, `aws-sdk-go-v2` (sqs, dynamodb), `pgx/v5`, PostgreSQL 16, LocalStack, Docker Compose. Sólo `testing` de la biblioteca estándar para tests.

**Spec:** `docs/specs/2026-11-08-consultas-diarias-design.md`

## Restricciones globales

- Módulo Go: `consultas-diarias`. Go 1.23. Sin frameworks web; `net/http` estándar.
- Nombres de paquetes, tipos y funciones en castellano, igual que la spec (`Clasificar`, `EncolarFranja`, `corridas_diarias`).
- Franjas: 288 por día. Intentos máximos: 5. Backoff: 30 s, 2 min, 8 min, 30 min, jitter ±20 %. Timeout HTTP: 30 s. Visibility timeout: 60 s.
- Estados de resultado: `ok`, `fallo_definitivo`, `agotado`.
- Tests de integración llevan `//go:build integration` y se corren con `go test -tags integration ./...` con compose levantado.
- Variables de entorno de AWS para LocalStack: `AWS_ENDPOINT_URL=http://localstack:4566`, `AWS_REGION=us-east-1`, `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`.
- Mensajes de commit: una línea en castellano describiendo el cambio. Nada más.
- Ningún archivo del repositorio menciona herramientas de asistencia ni generación automática.

---

## Estructura de archivos

| Archivo | Responsabilidad |
|---|---|
| `go.mod`, `go.sum` | Módulo y dependencias |
| `Makefile` | `up`, `down`, `test`, `test-integration`, `seed`, `demo` |
| `deploy/docker-compose.yml` | Postgres, LocalStack, api-mock, scheduler, worker, seed |
| `deploy/init.sql` | Esquema Postgres (sección 7.1 de la spec) |
| `deploy/localstack-init.sh` | Crea cola FIFO, DLQ y tabla DynamoDB al arrancar LocalStack |
| `deploy/Dockerfile` | Imagen multi-stage que compila los cuatro binarios |
| `internal/dominio/tipos.go` | `Registro`, `Mensaje`, `MensajeRecibido`, `Resultado`, `Clasificacion`, constantes |
| `internal/worker/clasificar.go` | `Clasificar(status, body, err)` |
| `internal/worker/backoff.go` | `Backoff(intento, rnd)` |
| `internal/worker/procesar.go` | `Procesador.Procesar(ctx, msg)` |
| `internal/worker/*_test.go` | Unitarios del worker |
| `internal/scheduler/slots.go` | `AsignarSlots(regs, franjas)` y `SlotActual(t, escala)` |
| `internal/scheduler/franja.go` | `Scheduler.EncolarFranja(ctx, fecha, slot)` |
| `internal/scheduler/*_test.go` | Unitarios del scheduler |
| `internal/cola/cola.go` | Interfaz `Cola` |
| `internal/cola/fake.go` | `Fake` en memoria para tests |
| `internal/cola/sqs.go` | Adaptador SQS FIFO |
| `internal/resultados/resultados.go` | Interfaz `Resultados`, `ErrYaExiste` |
| `internal/resultados/fake.go` | `Fake` en memoria |
| `internal/resultados/dynamo.go` | Adaptador DynamoDB |
| `internal/fuente/fuente.go` | Interfaz `Fuente` |
| `internal/fuente/fake.go` | `Fake` en memoria |
| `internal/fuente/postgres.go` | Adaptador Postgres con pgx |
| `internal/apiclient/cliente.go` | Interfaz `ClienteAPI`, `Fake`, implementación HTTP |
| `internal/awscfg/awscfg.go` | Carga de config del SDK apuntando a LocalStack |
| `cmd/api-mock/main.go` | Servidor HTTP que simula la API interna |
| `cmd/api-mock/guardia.go` | Guardia de concurrencia por proveedor y `/stats` |
| `cmd/worker/main.go` | Loop de polling con N goroutines y apagado ordenado |
| `cmd/scheduler/main.go` | Ticker con `ESCALA_TIEMPO` que llama a `EncolarFranja` |
| `cmd/seed/main.go` | Dataset sintético Pareto y cálculo de slots |
| `tests/integracion/*_test.go` | Integración contra compose |
| `README.md` | Diagramas, decisiones, cómo correr, resultados de la demo |

---

### Tarea 1: Esqueleto del repositorio y compose

**Archivos:**
- Crear: `go.mod`, `Makefile`, `.gitignore`, `deploy/docker-compose.yml`, `deploy/init.sql`, `deploy/localstack-init.sh`, `deploy/Dockerfile`, `cmd/api-mock/main.go` (stub), `cmd/worker/main.go` (stub), `cmd/scheduler/main.go` (stub), `cmd/seed/main.go` (stub)

**Interfaces:**
- Produce: cola `consultas.fifo`, DLQ `consultas-dlq.fifo` con `maxReceiveCount=5`, tabla DynamoDB `resultados` (PK `record_id` N, SK `fecha` S, GSI `proveedor-fecha-index`), esquema Postgres. Nombres de servicio en compose: `postgres`, `localstack`, `api-mock`, `worker`, `scheduler`, `seed`.

- [ ] **Paso 1: Inicializar el módulo y dependencias**

```bash
go mod init consultas-diarias
go get github.com/aws/aws-sdk-go-v2@latest \
       github.com/aws/aws-sdk-go-v2/config@latest \
       github.com/aws/aws-sdk-go-v2/service/sqs@latest \
       github.com/aws/aws-sdk-go-v2/service/dynamodb@latest \
       github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue@latest \
       github.com/jackc/pgx/v5@latest
```

- [ ] **Paso 2: Stubs de los cuatro binarios**

Cada `cmd/*/main.go` con este contenido (cambiando el nombre):

```go
package main

import "fmt"

func main() { fmt.Println("worker: pendiente") }
```

- [ ] **Paso 3: `.gitignore`**

```
/bin/
*.out
.env
```

- [ ] **Paso 4: `deploy/init.sql`**

```sql
CREATE TABLE IF NOT EXISTS registros (
  id         BIGSERIAL PRIMARY KEY,
  proveedor  TEXT        NOT NULL,
  endpoint   TEXT        NOT NULL,
  body       JSONB       NOT NULL,
  slot       SMALLINT    NOT NULL DEFAULT 0,
  activo     BOOLEAN     NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS registros_slot_idx ON registros (slot, proveedor) WHERE activo;

CREATE TABLE IF NOT EXISTS corridas_diarias (
  fecha         DATE        NOT NULL,
  slot          SMALLINT    NOT NULL,
  estado        TEXT        NOT NULL,
  encolados     INT         NOT NULL DEFAULT 0,
  iniciada_en   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finalizada_en TIMESTAMPTZ,
  PRIMARY KEY (fecha, slot)
);

CREATE TABLE IF NOT EXISTS proveedores_estado (
  proveedor       TEXT PRIMARY KEY,
  latencia_p50_ms INT,
  latencia_p95_ms INT,
  saturacion      NUMERIC(4,2),
  pendientes_ayer INT NOT NULL DEFAULT 0,
  actualizado_en  TIMESTAMPTZ
);
```

- [ ] **Paso 5: `deploy/localstack-init.sh`**

LocalStack ejecuta los scripts de `/etc/localstack/init/ready.d/` al estar listo.

```bash
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
```

- [ ] **Paso 6: `deploy/Dockerfile`**

```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ /usr/local/bin/
```

- [ ] **Paso 7: `deploy/docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: consultas
    ports: ["5432:5432"]
    volumes:
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d consultas"]
      interval: 2s
      retries: 30

  localstack:
    image: localstack/localstack:3
    environment:
      SERVICES: sqs,dynamodb
      DEBUG: 0
    ports: ["4566:4566"]
    volumes:
      - ./localstack-init.sh:/etc/localstack/init/ready.d/init.sh:ro
    healthcheck:
      test: ["CMD-SHELL", "awslocal sqs get-queue-url --queue-name consultas.fifo"]
      interval: 3s
      retries: 30

  api-mock:
    build: { context: .., dockerfile: deploy/Dockerfile }
    command: api-mock
    environment:
      PUERTO: "8080"
      LATENCIA_BASE_MS: "200"
      LATENCIA_VAR_MS: "100"
      PROB_500: "0.02"
      PROB_503: "0.02"
      PROB_429: "0.01"
      PROB_TIMEOUT: "0.005"
      PROB_400: "0.01"
      MAX_CONCURRENCIA_PROVEEDOR: "1"
    ports: ["8080:8080"]

  worker:
    build: { context: .., dockerfile: deploy/Dockerfile }
    command: worker
    environment: &aws
      AWS_ENDPOINT_URL: http://localstack:4566
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      COLA_URL: http://localstack:4566/000000000000/consultas.fifo
      TABLA_RESULTADOS: resultados
      API_BASE_URL: http://api-mock:8080
      CONCURRENCIA: "10"
      ESCALA_TIEMPO: "1"
    depends_on:
      localstack: { condition: service_healthy }
      api-mock: { condition: service_started }

  scheduler:
    build: { context: .., dockerfile: deploy/Dockerfile }
    command: scheduler
    environment:
      <<: *aws
      POSTGRES_URL: postgres://app:app@postgres:5432/consultas
      ESCALA_TIEMPO: "1"
    depends_on:
      postgres: { condition: service_healthy }
      localstack: { condition: service_healthy }

  seed:
    build: { context: .., dockerfile: deploy/Dockerfile }
    command: seed
    profiles: ["seed"]
    environment:
      POSTGRES_URL: postgres://app:app@postgres:5432/consultas
      REGISTROS: "100000"
      PROVEEDORES: "200"
    depends_on:
      postgres: { condition: service_healthy }
```

- [ ] **Paso 8: `Makefile`**

```makefile
COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: up down infra test test-integration seed demo logs

infra:
	$(COMPOSE) up -d postgres localstack api-mock

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

test:
	go test ./...

test-integration: infra
	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
	POSTGRES_URL=postgres://app:app@localhost:5432/consultas \
	API_BASE_URL=http://localhost:8080 \
	go test -tags integration -count=1 ./tests/...

seed:
	$(COMPOSE) run --rm seed

logs:
	$(COMPOSE) logs -f worker scheduler
```

- [ ] **Paso 9: Verificar que la infraestructura levanta**

```bash
make infra
docker compose -f deploy/docker-compose.yml ps
docker compose -f deploy/docker-compose.yml exec localstack awslocal sqs list-queues
docker compose -f deploy/docker-compose.yml exec postgres psql -U app -d consultas -c '\dt'
```

Esperado: `postgres`, `localstack` y `api-mock` en estado healthy/running; las dos colas listadas; las tres tablas presentes.

- [ ] **Paso 10: Commit**

```bash
git add go.mod go.sum Makefile .gitignore deploy cmd
git commit -m "Esqueleto del proyecto con compose, Postgres y LocalStack"
```

---

### Tarea 2: Tipos de dominio y clasificación de respuestas

**Archivos:**
- Crear: `internal/dominio/tipos.go`, `internal/worker/clasificar.go`
- Test: `internal/worker/clasificar_test.go`

**Interfaces:**
- Produce:
  ```go
  type Clasificacion int
  const ( Exito Clasificacion = iota; Reintentable; NoReintentable )
  func Clasificar(status int, body []byte, err error) Clasificacion
  ```
  y los tipos de `dominio` que usan todas las tareas siguientes.

- [ ] **Paso 1: `internal/dominio/tipos.go`**

```go
// Package dominio define los tipos compartidos por scheduler y worker.
package dominio

import (
	"encoding/json"
	"time"
)

const (
	FranjasPorDia  = 288
	IntentosMax    = 5
	TimeoutHTTP    = 30 * time.Second
	RetencionDias  = 90
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
```

- [ ] **Paso 2: Test de `Clasificar`**

`internal/worker/clasificar_test.go`:

```go
package worker

import (
	"context"
	"errors"
	"net"
	"testing"

	"consultas-diarias/internal/dominio"
)

func TestClasificar(t *testing.T) {
	casos := []struct {
		nombre string
		status int
		body   string
		err    error
		quiero dominio.Clasificacion
	}{
		{"200 con json", 200, `{"ok":true}`, nil, dominio.Exito},
		{"201", 201, `{}`, nil, dominio.Exito},
		{"200 con body invalido", 200, `no es json`, nil, dominio.NoReintentable},
		{"400", 400, ``, nil, dominio.NoReintentable},
		{"401", 401, ``, nil, dominio.NoReintentable},
		{"403", 403, ``, nil, dominio.NoReintentable},
		{"404", 404, ``, nil, dominio.NoReintentable},
		{"405", 405, ``, nil, dominio.NoReintentable},
		{"422", 422, ``, nil, dominio.NoReintentable},
		{"418 otro 4xx", 418, ``, nil, dominio.NoReintentable},
		{"429", 429, ``, nil, dominio.Reintentable},
		{"500", 500, ``, nil, dominio.Reintentable},
		{"502", 502, ``, nil, dominio.Reintentable},
		{"503", 503, ``, nil, dominio.Reintentable},
		{"504", 504, ``, nil, dominio.Reintentable},
		{"timeout", 0, ``, context.DeadlineExceeded, dominio.Reintentable},
		{"error de red", 0, ``, &net.OpError{Op: "dial", Err: errors.New("refused")}, dominio.Reintentable},
		{"error generico", 0, ``, errors.New("algo"), dominio.Reintentable},
	}
	for _, c := range casos {
		t.Run(c.nombre, func(t *testing.T) {
			got := Clasificar(c.status, []byte(c.body), c.err)
			if got != c.quiero {
				t.Fatalf("Clasificar(%d, %q, %v) = %s, quiero %s", c.status, c.body, c.err, got, c.quiero)
			}
		})
	}
}
```

- [ ] **Paso 3: Correr y ver que falla**

```bash
go test ./internal/worker/ -run TestClasificar
```

Esperado: error de compilación, `Clasificar` no definido.

- [ ] **Paso 4: Implementar `internal/worker/clasificar.go`**

```go
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
```

- [ ] **Paso 5: Correr y ver que pasa**

```bash
go test ./internal/worker/ -run TestClasificar -v
```

Esperado: todos los subtests PASS.

- [ ] **Paso 6: Commit**

```bash
git add internal/dominio internal/worker
git commit -m "Tipos de dominio y clasificación de respuestas de la API"
```

---

### Tarea 3: Backoff con jitter

**Archivos:**
- Crear: `internal/worker/backoff.go`
- Test: `internal/worker/backoff_test.go`

**Interfaces:**
- Produce: `func Backoff(intento int, rnd func() float64) (espera time.Duration, reintentar bool)`. `intento` es 1 en la primera entrega. `rnd` devuelve un valor en `[0,1)`; se inyecta para que los tests sean deterministas. Devuelve `reintentar=false` cuando `intento >= dominio.IntentosMax`.

- [ ] **Paso 1: Test**

`internal/worker/backoff_test.go`:

```go
package worker

import (
	"testing"
	"time"
)

func TestBackoffBase(t *testing.T) {
	medio := func() float64 { return 0.5 } // jitter neutro
	casos := []struct {
		intento int
		espera  time.Duration
		sigue   bool
	}{
		{1, 30 * time.Second, true},
		{2, 2 * time.Minute, true},
		{3, 8 * time.Minute, true},
		{4, 30 * time.Minute, true},
		{5, 0, false},
		{9, 0, false},
	}
	for _, c := range casos {
		espera, sigue := Backoff(c.intento, medio)
		if espera != c.espera || sigue != c.sigue {
			t.Errorf("Backoff(%d) = (%v, %v), quiero (%v, %v)", c.intento, espera, sigue, c.espera, c.sigue)
		}
	}
}

func TestBackoffJitterDentroDelRango(t *testing.T) {
	min, _ := Backoff(2, func() float64 { return 0 })
	max, _ := Backoff(2, func() float64 { return 0.999999 })
	base := 2 * time.Minute
	if min < time.Duration(float64(base)*0.8) || min > base {
		t.Errorf("jitter mínimo fuera de rango: %v", min)
	}
	if max > time.Duration(float64(base)*1.2) || max < base {
		t.Errorf("jitter máximo fuera de rango: %v", max)
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./internal/worker/ -run TestBackoff
```

Esperado: `Backoff` no definido.

- [ ] **Paso 3: Implementar `internal/worker/backoff.go`**

```go
package worker

import (
	"time"

	"consultas-diarias/internal/dominio"
)

var esperas = []time.Duration{
	30 * time.Second,
	2 * time.Minute,
	8 * time.Minute,
	30 * time.Minute,
}

// Backoff devuelve cuánto esperar antes del siguiente intento y si
// corresponde reintentar. El jitter es ±20 % para que los reintentos de
// distintos proveedores no se alineen.
func Backoff(intento int, rnd func() float64) (time.Duration, bool) {
	if intento < 1 || intento >= dominio.IntentosMax {
		return 0, false
	}
	base := esperas[intento-1]
	factor := 0.8 + 0.4*rnd()
	return time.Duration(float64(base) * factor), true
}
```

- [ ] **Paso 4: Correr y ver que pasa**

```bash
go test ./internal/worker/ -v
```

Esperado: PASS.

- [ ] **Paso 5: Commit**

```bash
git add internal/worker/backoff.go internal/worker/backoff_test.go
git commit -m "Backoff exponencial con jitter para reintentos"
```

---

### Tarea 4: Interfaces de infraestructura y fakes en memoria

**Archivos:**
- Crear: `internal/cola/cola.go`, `internal/cola/fake.go`, `internal/resultados/resultados.go`, `internal/resultados/fake.go`, `internal/fuente/fuente.go`, `internal/fuente/fake.go`, `internal/apiclient/cliente.go`
- Test: `internal/cola/fake_test.go`

**Interfaces:**
- Produce (las usan todas las tareas siguientes):

```go
// cola
type Cola interface {
	Enviar(ctx context.Context, msgs []dominio.Mensaje) error
	Recibir(ctx context.Context, max int) ([]dominio.MensajeRecibido, error)
	Borrar(ctx context.Context, recibo string) error
	CambiarVisibilidad(ctx context.Context, recibo string, espera time.Duration) error
}
// resultados
var ErrYaExiste = errors.New("resultado ya registrado")
type Resultados interface {
	Guardar(ctx context.Context, r dominio.Resultado) error // ErrYaExiste si hay estado final
}
// fuente
type Fuente interface {
	RegistrosDeSlot(ctx context.Context, slot int) ([]dominio.Registro, error)
	IniciarFranja(ctx context.Context, fecha string, slot int) (bool, error) // false si ya existía
	CerrarFranja(ctx context.Context, fecha string, slot int, encolados int) error
	ProveedoresAtrasados(ctx context.Context) ([]string, error)
}
// apiclient
type ClienteAPI interface {
	Consultar(ctx context.Context, proveedor, endpoint string, body []byte) (status int, resp []byte, err error)
}
```

- [ ] **Paso 1: `internal/cola/cola.go`**

```go
// Package cola abstrae la cola FIFO por proveedor.
package cola

import (
	"context"
	"time"

	"consultas-diarias/internal/dominio"
)

// Cola garantiza que de un mismo proveedor hay a lo sumo un mensaje
// entregado y no borrado ni expirado a la vez.
type Cola interface {
	Enviar(ctx context.Context, msgs []dominio.Mensaje) error
	Recibir(ctx context.Context, max int) ([]dominio.MensajeRecibido, error)
	Borrar(ctx context.Context, recibo string) error
	CambiarVisibilidad(ctx context.Context, recibo string, espera time.Duration) error
}
```

- [ ] **Paso 2: `internal/cola/fake.go`**

Reproduce la semántica FIFO por grupo: un proveedor con un mensaje en vuelo no entrega otro. Sin tiempos reales: `CambiarVisibilidad` devuelve el mensaje a la cola de inmediato con `Intento+1`, lo que alcanza para los tests de lógica.

```go
package cola

import (
	"context"
	"fmt"
	"sync"
	"time"

	"consultas-diarias/internal/dominio"
)

// Fake es una cola en memoria con semántica FIFO por proveedor.
type Fake struct {
	mu        sync.Mutex
	pendiente []dominio.MensajeRecibido
	enVuelo   map[string]dominio.MensajeRecibido // recibo -> mensaje
	seq       int
	Borrados  []int64 // registro_id de los mensajes borrados, en orden
	Enviados  int
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

func (f *Fake) Recibir(_ context.Context, max int) ([]dominio.MensajeRecibido, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bloqueados := map[string]bool{}
	for _, m := range f.enVuelo {
		bloqueados[m.Proveedor] = true
	}
	var salida []dominio.MensajeRecibido
	var resto []dominio.MensajeRecibido
	for _, m := range f.pendiente {
		if len(salida) < max && !bloqueados[m.Proveedor] {
			f.seq++
			m.Recibo = fmt.Sprintf("r%d", f.seq)
			f.enVuelo[m.Recibo] = m
			bloqueados[m.Proveedor] = true
			salida = append(salida, m)
		} else {
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

func (f *Fake) CambiarVisibilidad(_ context.Context, recibo string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.enVuelo[recibo]
	if !ok {
		return fmt.Errorf("recibo desconocido: %s", recibo)
	}
	delete(f.enVuelo, recibo)
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
```

- [ ] **Paso 3: Test del fake (la semántica por proveedor es lo que importa)**

`internal/cola/fake_test.go`:

```go
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
```

- [ ] **Paso 4: Correr**

```bash
go test ./internal/cola/ -v
```

Esperado: PASS.

- [ ] **Paso 5: `internal/resultados/resultados.go` y `fake.go`**

```go
// Package resultados persiste el resultado de cada consulta diaria.
package resultados

import (
	"context"
	"errors"

	"consultas-diarias/internal/dominio"
)

// ErrYaExiste indica que ya hay un resultado con estado final para ese
// registro y fecha. El llamador debe tratarlo como éxito idempotente.
var ErrYaExiste = errors.New("resultado ya registrado")

type Resultados interface {
	Guardar(ctx context.Context, r dominio.Resultado) error
}
```

```go
package resultados

import (
	"context"
	"fmt"
	"sync"

	"consultas-diarias/internal/dominio"
)

type Fake struct {
	mu       sync.Mutex
	Items    map[string]dominio.Resultado // "id:fecha"
	FallarN  int                          // próximas N escrituras fallan con error genérico
}

func NuevaFake() *Fake { return &Fake{Items: map[string]dominio.Resultado{}} }

func clave(r dominio.Resultado) string { return fmt.Sprintf("%d:%s", r.RegistroID, r.Fecha) }

func (f *Fake) Guardar(_ context.Context, r dominio.Resultado) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FallarN > 0 {
		f.FallarN--
		return fmt.Errorf("fallo simulado")
	}
	if _, ok := f.Items[clave(r)]; ok {
		return ErrYaExiste
	}
	f.Items[clave(r)] = r
	return nil
}
```

- [ ] **Paso 6: `internal/fuente/fuente.go` y `fake.go`**

```go
// Package fuente lee la tabla de registros y lleva el estado de las franjas.
package fuente

import (
	"context"

	"consultas-diarias/internal/dominio"
)

type Fuente interface {
	// RegistrosDeSlot devuelve los registros activos de la franja.
	RegistrosDeSlot(ctx context.Context, slot int) ([]dominio.Registro, error)
	// IniciarFranja inserta (fecha, slot) en corridas_diarias. Devuelve
	// false si ya existía: otra ejecución la tomó.
	IniciarFranja(ctx context.Context, fecha string, slot int) (bool, error)
	CerrarFranja(ctx context.Context, fecha string, slot int, encolados int) error
	// ProveedoresAtrasados devuelve los proveedores con pendientes del día
	// anterior, que no deben encolarse hoy.
	ProveedoresAtrasados(ctx context.Context) ([]string, error)
}
```

```go
package fuente

import (
	"context"
	"fmt"
	"sync"

	"consultas-diarias/internal/dominio"
)

type Fake struct {
	mu         sync.Mutex
	Registros  []dominio.Registro
	Franjas    map[string]int // "fecha:slot" -> encolados (-1 = iniciada, sin cerrar)
	Atrasados  []string
}

func NuevaFake(regs []dominio.Registro) *Fake {
	return &Fake{Registros: regs, Franjas: map[string]int{}}
}

func (f *Fake) RegistrosDeSlot(_ context.Context, slot int) ([]dominio.Registro, error) {
	var out []dominio.Registro
	for _, r := range f.Registros {
		if r.Slot == slot {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *Fake) IniciarFranja(_ context.Context, fecha string, slot int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := fmt.Sprintf("%s:%d", fecha, slot)
	if _, ok := f.Franjas[k]; ok {
		return false, nil
	}
	f.Franjas[k] = -1
	return true, nil
}

func (f *Fake) CerrarFranja(_ context.Context, fecha string, slot int, encolados int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Franjas[fmt.Sprintf("%s:%d", fecha, slot)] = encolados
	return nil
}

func (f *Fake) ProveedoresAtrasados(_ context.Context) ([]string, error) {
	return f.Atrasados, nil
}
```

- [ ] **Paso 7: `internal/apiclient/cliente.go` (interfaz y fake; la implementación HTTP va en la Tarea 7)**

```go
// Package apiclient llama a la API interna.
package apiclient

import (
	"context"
	"sync"
)

type ClienteAPI interface {
	// Consultar devuelve el status HTTP y el body. err distinto de nil sólo
	// para fallas de transporte (timeout, conexión); un 500 no es error.
	Consultar(ctx context.Context, proveedor, endpoint string, body []byte) (int, []byte, error)
}

// Respuesta es un resultado programado del Fake.
type Respuesta struct {
	Status int
	Body   []byte
	Err    error
}

// Fake devuelve respuestas en orden; repite la última cuando se agotan.
type Fake struct {
	mu        sync.Mutex
	Programa  []Respuesta
	Llamadas  int
}

func (f *Fake) Consultar(_ context.Context, _, _ string, _ []byte) (int, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.Llamadas
	if i >= len(f.Programa) {
		i = len(f.Programa) - 1
	}
	f.Llamadas++
	r := f.Programa[i]
	return r.Status, r.Body, r.Err
}
```

- [ ] **Paso 8: Compilar todo y commit**

```bash
go build ./... && go test ./...
git add internal
git commit -m "Interfaces de cola, fuente, resultados y cliente de API con fakes"
```

---

### Tarea 5: Procesamiento de un mensaje

**Archivos:**
- Crear: `internal/worker/procesar.go`
- Test: `internal/worker/procesar_test.go`

**Interfaces:**
- Consume: `cola.Cola`, `resultados.Resultados`, `apiclient.ClienteAPI`, `Clasificar`, `Backoff`.
- Produce:
  ```go
  type Procesador struct {
      Cola       cola.Cola
      Resultados resultados.Resultados
      API        apiclient.ClienteAPI
      Ahora      func() time.Time
      Rnd        func() float64
      Escala     float64 // divide las esperas de backoff; 1 en producción
  }
  func (p *Procesador) Procesar(ctx context.Context, m dominio.MensajeRecibido) error
  ```

- [ ] **Paso 1: Test con fakes**

`internal/worker/procesar_test.go`:

```go
package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

func armar(prog ...apiclient.Respuesta) (*Procesador, *cola.Fake, *resultados.Fake) {
	c := cola.NuevaFake()
	r := resultados.NuevaFake()
	p := &Procesador{
		Cola:       c,
		Resultados: r,
		API:        &apiclient.Fake{Programa: prog},
		Ahora:      func() time.Time { return time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC) },
		Rnd:        func() float64 { return 0.5 },
		Escala:     1,
	}
	return p, c, r
}

func entregar(t *testing.T, c *cola.Fake) dominio.MensajeRecibido {
	t.Helper()
	ctx := context.Background()
	_ = c.Enviar(ctx, []dominio.Mensaje{{RegistroID: 7, Proveedor: "acme", Endpoint: "/x", Body: []byte(`{}`), Fecha: "2026-11-08"}})
	got, _ := c.Recibir(ctx, 1)
	return got[0]
}

func TestProcesarExito(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 200, Body: []byte(`{"v":1}`)})
	m := entregar(t, c)
	if err := p.Procesar(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	res := r.Items["7:2026-11-08"]
	if res.Estado != dominio.EstadoOK || res.HTTPStatus != 200 || res.Intentos != 1 || string(res.Respuesta) != `{"v":1}` {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(c.Borrados) != 1 {
		t.Fatalf("el mensaje debía borrarse")
	}
}

func TestProcesarNoReintentable(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 400, Body: []byte(`{"error":"body invalido"}`)})
	m := entregar(t, c)
	_ = p.Procesar(context.Background(), m)
	res := r.Items["7:2026-11-08"]
	if res.Estado != dominio.EstadoFalloDefinitivo || res.HTTPStatus != 400 || res.Error == "" {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(c.Borrados) != 1 {
		t.Fatal("un 400 se borra sin reintentar")
	}
}

func TestProcesarReintentableReprograma(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 503})
	m := entregar(t, c)
	_ = p.Procesar(context.Background(), m)
	if len(r.Items) != 0 {
		t.Fatal("no debe guardar resultado en un reintento")
	}
	if len(c.Borrados) != 0 || c.Pendientes() != 1 {
		t.Fatal("el mensaje debía volver a la cola")
	}
	otra, _ := c.Recibir(context.Background(), 1)
	if otra[0].Intento != 2 {
		t.Fatalf("intento = %d", otra[0].Intento)
	}
}

func TestProcesarAgotado(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 503})
	m := entregar(t, c)
	m.Intento = dominio.IntentosMax
	_ = p.Procesar(context.Background(), m)
	res := r.Items["7:2026-11-08"]
	if res.Estado != dominio.EstadoAgotado || res.Intentos != 5 {
		t.Fatalf("resultado inesperado: %+v", res)
	}
	if len(c.Borrados) != 0 {
		t.Fatal("al agotar no se borra: SQS lo mueve a la DLQ por maxReceiveCount")
	}
}

func TestProcesarTimeoutEsReintentable(t *testing.T) {
	p, c, _ := armar(apiclient.Respuesta{Err: context.DeadlineExceeded})
	m := entregar(t, c)
	_ = p.Procesar(context.Background(), m)
	if c.Pendientes() != 1 {
		t.Fatal("timeout debía reprogramar")
	}
}

func TestProcesarResultadoYaExisteBorraIgual(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 200, Body: []byte(`{}`)})
	m := entregar(t, c)
	r.Items["7:2026-11-08"] = dominio.Resultado{Estado: dominio.EstadoOK}
	if err := p.Procesar(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(c.Borrados) != 1 {
		t.Fatal("si el resultado ya estaba, el mensaje es un duplicado y se borra")
	}
}

func TestProcesarFalloAlGuardarReprograma(t *testing.T) {
	p, c, r := armar(apiclient.Respuesta{Status: 200, Body: []byte(`{}`)})
	r.FallarN = 1
	m := entregar(t, c)
	err := p.Procesar(context.Background(), m)
	if err == nil || c.Pendientes() != 1 {
		t.Fatalf("fallo al guardar debe devolver error y reprogramar; err=%v", err)
	}
	if errors.Is(err, resultados.ErrYaExiste) {
		t.Fatal("no es ErrYaExiste")
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./internal/worker/ -run TestProcesar
```

Esperado: `Procesador` no definido.

- [ ] **Paso 3: Implementar `internal/worker/procesar.go`**

```go
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

// Procesar nunca deja un mensaje en limbo: lo borra, lo reprograma o lo
// deja vencer para que SQS lo mueva a la DLQ.
func (p *Procesador) Procesar(ctx context.Context, m dominio.MensajeRecibido) error {
	inicio := p.Ahora()
	ctxAPI, cancel := context.WithTimeout(ctx, dominio.TimeoutHTTP)
	defer cancel()
	status, body, err := p.API.Consultar(ctxAPI, m.Proveedor, m.Endpoint, m.Body)
	latencia := time.Since(inicio).Milliseconds()

	res := dominio.Resultado{
		RegistroID:  m.RegistroID,
		Fecha:       m.Fecha,
		Proveedor:   m.Proveedor,
		HTTPStatus:  status,
		Intentos:    m.Intento,
		LatenciaMs:  latencia,
		ProcesadoEn: p.Ahora(),
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
			return nil // no borramos: SQS lo manda a la DLQ
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
	if espera == 0 {
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
```

- [ ] **Paso 4: Correr y ver que pasa**

```bash
go test ./internal/worker/ -v
```

Esperado: los siete `TestProcesar*` PASS más los anteriores.

- [ ] **Paso 5: Commit**

```bash
git add internal/worker
git commit -m "Procesamiento de mensajes con clasificación, reintentos y persistencia"
```

---

### Tarea 6: Asignación de slots y slot actual

**Archivos:**
- Crear: `internal/scheduler/slots.go`
- Test: `internal/scheduler/slots_test.go`

**Interfaces:**
- Produce:
  ```go
  func AsignarSlots(regs []dominio.Registro, franjas int) map[int64]int
  func SlotActual(t time.Time, inicio time.Time, escala float64) (slot int, fecha string)
  ```
  `AsignarSlots` es la versión en Go de la consulta SQL de la spec (5.2); se usa en tests y en el seed. `SlotActual` devuelve la franja que corresponde al instante `t`: con `escala=1` es `(hora*60+min)/5` del día de `t`; con `escala>1` el día se comprime y empieza en `inicio`, de modo que `escala=150` hace que una franja dure 2 segundos.

- [ ] **Paso 1: Test**

`internal/scheduler/slots_test.go`:

```go
package scheduler

import (
	"testing"
	"time"

	"consultas-diarias/internal/dominio"
)

func TestAsignarSlotsUniformePorProveedor(t *testing.T) {
	var regs []dominio.Registro
	id := int64(1)
	for i := 0; i < 2880; i++ { // proveedor grande: 10 por franja
		regs = append(regs, dominio.Registro{ID: id, Proveedor: "grande"})
		id++
	}
	for i := 0; i < 3; i++ { // proveedor chico: 3 en todo el día
		regs = append(regs, dominio.Registro{ID: id, Proveedor: "chico"})
		id++
	}
	slots := AsignarSlots(regs, 288)

	porFranja := map[int]int{}
	for _, r := range regs {
		if r.Proveedor == "grande" {
			porFranja[slots[r.ID]]++
		}
	}
	for f := 0; f < 288; f++ {
		if porFranja[f] != 10 {
			t.Fatalf("franja %d del proveedor grande tiene %d, quiero 10", f, porFranja[f])
		}
	}
	chico := []int{slots[2881], slots[2882], slots[2883]}
	if chico[0] != 0 || chico[1] != 96 || chico[2] != 192 {
		t.Fatalf("proveedor chico mal repartido: %v", chico)
	}
}

func TestSlotActualSinEscala(t *testing.T) {
	cualquiera := time.Time{}
	slot, fecha := SlotActual(time.Date(2026, 11, 8, 13, 7, 0, 0, time.UTC), cualquiera, 1)
	if slot != 157 || fecha != "2026-11-08" {
		t.Fatalf("got %d %s", slot, fecha)
	}
	slot, _ = SlotActual(time.Date(2026, 11, 8, 23, 59, 59, 0, time.UTC), cualquiera, 1)
	if slot != 287 {
		t.Fatalf("got %d", slot)
	}
}

func TestSlotActualComprimido(t *testing.T) {
	inicio := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	// escala 150: una franja de 300 s dura 2 s; el día dura 576 s.
	slot, fecha := SlotActual(inicio.Add(5*time.Second), inicio, 150)
	if slot != 2 || fecha != "2026-11-08" {
		t.Fatalf("got %d %s", slot, fecha)
	}
	slot, fecha = SlotActual(inicio.Add(600*time.Second), inicio, 150)
	if slot != 12 || fecha != "2026-11-09" {
		t.Fatalf("segundo día comprimido: got %d %s", slot, fecha)
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./internal/scheduler/
```

- [ ] **Paso 3: Implementar `internal/scheduler/slots.go`**

```go
// Package scheduler decide qué registros se encolan en cada franja.
package scheduler

import (
	"sort"
	"time"

	"consultas-diarias/internal/dominio"
)

// AsignarSlots reparte los registros de cada proveedor de forma uniforme
// en las franjas del día: slot = rank * franjas / total, ordenando por id.
func AsignarSlots(regs []dominio.Registro, franjas int) map[int64]int {
	porProveedor := map[string][]int64{}
	for _, r := range regs {
		porProveedor[r.Proveedor] = append(porProveedor[r.Proveedor], r.ID)
	}
	out := make(map[int64]int, len(regs))
	for _, ids := range porProveedor {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		total := len(ids)
		for rank, id := range ids {
			out[id] = rank * franjas / total
		}
	}
	return out
}

const duracionFranja = 24 * time.Hour / dominio.FranjasPorDia

// SlotActual devuelve la franja y la fecha lógica para el instante t.
// Con escala 1 usa el reloj real. Con escala > 1 el día se comprime y
// arranca en inicio: t = inicio es slot 0 del día de inicio.
func SlotActual(t, inicio time.Time, escala float64) (int, string) {
	if escala <= 1 {
		desdeMedianoche := t.Sub(t.Truncate(24 * time.Hour))
		return int(desdeMedianoche / duracionFranja), t.Format("2006-01-02")
	}
	virtual := time.Duration(float64(t.Sub(inicio)) * escala)
	dias := int(virtual / (24 * time.Hour))
	resto := virtual % (24 * time.Hour)
	fecha := inicio.AddDate(0, 0, dias).Format("2006-01-02")
	return int(resto / duracionFranja), fecha
}
```

- [ ] **Paso 4: Correr y ver que pasa**

```bash
go test ./internal/scheduler/ -v
```

- [ ] **Paso 5: Commit**

```bash
git add internal/scheduler
git commit -m "Asignación uniforme de franjas por proveedor y cálculo de slot actual"
```

---

### Tarea 7: Adaptadores reales: SQS, DynamoDB, Postgres y cliente HTTP

**Archivos:**
- Crear: `internal/awscfg/awscfg.go`, `internal/cola/sqs.go`, `internal/resultados/dynamo.go`, `internal/fuente/postgres.go`, `internal/apiclient/http.go`
- Test: `tests/integracion/adaptadores_test.go` (tag `integration`)

**Interfaces:**
- Consume: las interfaces de la Tarea 4.
- Produce:
  ```go
  awscfg.Cargar(ctx) (aws.Config, error)
  cola.NuevaSQS(cliente *sqs.Client, url string) *SQS
  resultados.NuevaDynamo(cliente *dynamodb.Client, tabla string) *Dynamo
  fuente.NuevaPostgres(pool *pgxpool.Pool) *Postgres
  apiclient.NuevoHTTP(base string, timeout time.Duration) *HTTP
  ```

- [ ] **Paso 1: `internal/awscfg/awscfg.go`**

El SDK v2 lee `AWS_ENDPOINT_URL` solo, así que la configuración es la estándar. El paquete existe para no repetir el manejo de errores en cada `main`.

```go
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
```

- [ ] **Paso 2: `internal/cola/sqs.go`**

```go
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
// proveedor, lo que delega en SQS la serialización por proveedor.
type SQS struct {
	cliente *sqs.Client
	url     string
}

func NuevaSQS(cliente *sqs.Client, url string) *SQS {
	return &SQS{cliente: cliente, url: url}
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
				MessageDeduplicationId: aws.String(fmt.Sprintf("%d-%s", m.RegistroID, m.Fecha)),
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
	out, err := s.cliente.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.url),
		MaxNumberOfMessages: int32(min(max, 10)),
		WaitTimeSeconds:     20,
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameApproximateReceiveCount},
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
	segundos := int32(espera / time.Second)
	if segundos < 1 {
		segundos = 1
	}
	if segundos > 43200 {
		segundos = 43200
	}
	_, err := s.cliente.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.url),
		ReceiptHandle:     aws.String(recibo),
		VisibilityTimeout: segundos,
	})
	return err
}
```

- [ ] **Paso 3: `internal/resultados/dynamo.go`**

```go
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
```

- [ ] **Paso 4: `internal/fuente/postgres.go`**

```go
package fuente

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/dominio"
)

type Postgres struct{ pool *pgxpool.Pool }

func NuevaPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (p *Postgres) RegistrosDeSlot(ctx context.Context, slot int) ([]dominio.Registro, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, proveedor, endpoint, body, slot FROM registros WHERE slot = $1 AND activo ORDER BY proveedor, id`, slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []dominio.Registro
	for rows.Next() {
		var r dominio.Registro
		if err := rows.Scan(&r.ID, &r.Proveedor, &r.Endpoint, &r.Body, &r.Slot); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *Postgres) IniciarFranja(ctx context.Context, fecha string, slot int) (bool, error) {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO corridas_diarias (fecha, slot, estado) VALUES ($1, $2, 'encolando')`, fecha, slot)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("iniciar franja: %w", err)
	}
	return true, nil
}

func (p *Postgres) CerrarFranja(ctx context.Context, fecha string, slot int, encolados int) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE corridas_diarias SET estado = 'encolada', encolados = $3, finalizada_en = now() WHERE fecha = $1 AND slot = $2`,
		fecha, slot, encolados)
	return err
}

func (p *Postgres) ProveedoresAtrasados(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx, `SELECT proveedor FROM proveedores_estado WHERE pendientes_ayer > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
```

- [ ] **Paso 5: `internal/apiclient/http.go`**

```go
package apiclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTP llama a la API interna en {base}/{proveedor}{endpoint}.
type HTTP struct {
	base    string
	cliente *http.Client
}

func NuevoHTTP(base string, timeout time.Duration) *HTTP {
	return &HTTP{
		base:    strings.TrimRight(base, "/"),
		cliente: &http.Client{Timeout: timeout},
	}
}

func (h *HTTP) Consultar(ctx context.Context, proveedor, endpoint string, body []byte) (int, []byte, error) {
	url := h.base + "/" + proveedor + "/" + strings.TrimLeft(endpoint, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.cliente.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}
```

- [ ] **Paso 6: Test de integración de los adaptadores**

`tests/integracion/ayuda_test.go` (compartido por todos los tests de integración):

```go
//go:build integration

package integracion

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/awscfg"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/fuente"
	"consultas-diarias/internal/resultados"
)

type entorno struct {
	Cola       *cola.SQS
	Resultados *resultados.Dynamo
	Fuente     *fuente.Postgres
	Pool       *pgxpool.Pool
	SQS        *sqs.Client
	ColaURL    string
	DLQURL     string
	APIBase    string
}

func armarEntorno(t *testing.T) *entorno {
	t.Helper()
	ctx := context.Background()
	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		t.Fatal(err)
	}
	base := os.Getenv("AWS_ENDPOINT_URL") + "/000000000000/"
	sq := sqs.NewFromConfig(cfg)
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e := &entorno{
		Cola:       cola.NuevaSQS(sq, base+"consultas.fifo"),
		Resultados: resultados.NuevaDynamo(dynamodb.NewFromConfig(cfg), "resultados"),
		Fuente:     fuente.NuevaPostgres(pool),
		Pool:       pool,
		SQS:        sq,
		ColaURL:    base + "consultas.fifo",
		DLQURL:     base + "consultas-dlq.fifo",
		APIBase:    os.Getenv("API_BASE_URL"),
	}
	e.purgar(t)
	return e
}

// purgar vacía colas y tablas para que cada test arranque limpio.
func (e *entorno) purgar(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, u := range []string{e.ColaURL, e.DLQURL} {
		_, _ = e.SQS.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &u})
	}
	_, _ = e.Pool.Exec(ctx, `TRUNCATE registros, corridas_diarias, proveedores_estado`)
	time.Sleep(time.Second) // PurgeQueue es asíncrono
}

func esperar(t *testing.T, plazo time.Duration, cond func() bool) {
	t.Helper()
	limite := time.Now().Add(plazo)
	for time.Now().Before(limite) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("condición no alcanzada en el plazo")
}
```

`tests/integracion/adaptadores_test.go`:

```go
//go:build integration

package integracion

import (
	"context"
	"errors"
	"testing"
	"time"

	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

func TestSQSIdaYVuelta(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	msgs := []dominio.Mensaje{
		{RegistroID: 1, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Fecha: "2026-11-08"},
		{RegistroID: 2, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Fecha: "2026-11-08"},
	}
	if err := e.Cola.Enviar(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	got, err := e.Cola.Recibir(ctx, 10)
	if err != nil || len(got) != 1 || got[0].RegistroID != 1 || got[0].Intento != 1 {
		t.Fatalf("FIFO debía entregar sólo el primero del grupo: %+v, %v", got, err)
	}
	if err := e.Cola.CambiarVisibilidad(ctx, got[0].Recibo, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	otra, _ := e.Cola.Recibir(ctx, 10)
	if len(otra) != 1 || otra[0].RegistroID != 1 || otra[0].Intento != 2 {
		t.Fatalf("tras expirar debía volver el mismo con intento 2: %+v", otra)
	}
	if err := e.Cola.Borrar(ctx, otra[0].Recibo); err != nil {
		t.Fatal(err)
	}
	tercera, _ := e.Cola.Recibir(ctx, 10)
	if len(tercera) != 1 || tercera[0].RegistroID != 2 {
		t.Fatalf("debía llegar el segundo: %+v", tercera)
	}
}

func TestDynamoEscrituraCondicional(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	r := dominio.Resultado{RegistroID: time.Now().UnixNano(), Fecha: "2026-11-08", Proveedor: "a",
		Estado: dominio.EstadoOK, HTTPStatus: 200, Intentos: 1, Respuesta: []byte(`{}`), ProcesadoEn: time.Now()}
	if err := e.Resultados.Guardar(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := e.Resultados.Guardar(ctx, r); !errors.Is(err, resultados.ErrYaExiste) {
		t.Fatalf("segunda escritura debía fallar con ErrYaExiste, fue %v", err)
	}
	leido, ok, _ := e.Resultados.Leer(ctx, r.RegistroID, r.Fecha)
	if !ok || leido.Estado != dominio.EstadoOK {
		t.Fatalf("lectura: %+v %v", leido, ok)
	}
}

func TestPostgresFranjaIdempotente(t *testing.T) {
	e := armarEntorno(t)
	ctx := context.Background()
	ok, err := e.Fuente.IniciarFranja(ctx, "2026-11-08", 3)
	if err != nil || !ok {
		t.Fatalf("primera: %v %v", ok, err)
	}
	ok, err = e.Fuente.IniciarFranja(ctx, "2026-11-08", 3)
	if err != nil || ok {
		t.Fatalf("segunda debía devolver false: %v %v", ok, err)
	}
	_, _ = e.Pool.Exec(ctx, `INSERT INTO registros (proveedor, endpoint, body, slot) VALUES ('a','/x','{}',3),('b','/y','{}',3),('a','/z','{}',4)`)
	regs, err := e.Fuente.RegistrosDeSlot(ctx, 3)
	if err != nil || len(regs) != 2 {
		t.Fatalf("slot 3: %d registros, %v", len(regs), err)
	}
}
```

- [ ] **Paso 7: Correr**

```bash
make test-integration
```

Esperado: los tres tests PASS. Si `TestSQSIdaYVuelta` falla en el segundo `Recibir`, subí el `Sleep` a 2 s: LocalStack redondea el visibility timeout al segundo.

- [ ] **Paso 8: Commit**

```bash
git add internal tests
git commit -m "Adaptadores de SQS, DynamoDB, Postgres y cliente HTTP con tests de integración"
```

---

### Tarea 8: API mock con guardia de concurrencia

**Archivos:**
- Crear: `cmd/api-mock/guardia.go`, `cmd/api-mock/main.go` (reemplaza el stub)
- Test: `cmd/api-mock/guardia_test.go`

**Interfaces:**
- Produce: servidor HTTP en `PUERTO`. `POST /{proveedor}/{resto...}` responde según probabilidades de entorno. `GET /stats` devuelve JSON `{"max_en_vuelo": {"acme": 1, ...}, "violaciones": 0, "requests": 1234}`. Cualquier request que supere `MAX_CONCURRENCIA_PROVEEDOR` responde 409 y suma una violación.

- [ ] **Paso 1: Test de la guardia**

`cmd/api-mock/guardia_test.go`:

```go
package main

import (
	"sync"
	"testing"
)

func TestGuardiaPermiteHastaElLimite(t *testing.T) {
	g := nuevaGuardia(1)
	if !g.entrar("a") {
		t.Fatal("primera entrada debía pasar")
	}
	if g.entrar("a") {
		t.Fatal("segunda entrada simultánea debía rechazarse")
	}
	if !g.entrar("b") {
		t.Fatal("otro proveedor no se ve afectado")
	}
	g.salir("a")
	if !g.entrar("a") {
		t.Fatal("tras salir debía volver a pasar")
	}
	s := g.stats()
	if s.Violaciones != 1 || s.MaxEnVuelo["a"] != 1 || s.MaxEnVuelo["b"] != 1 {
		t.Fatalf("stats inesperadas: %+v", s)
	}
}

func TestGuardiaConcurrente(t *testing.T) {
	g := nuevaGuardia(1)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.entrar("a") {
				g.salir("a")
			}
		}()
	}
	wg.Wait()
	if g.stats().MaxEnVuelo["a"] != 1 {
		t.Fatalf("el máximo observado nunca debe superar 1: %+v", g.stats())
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./cmd/api-mock/
```

- [ ] **Paso 3: `cmd/api-mock/guardia.go`**

```go
package main

import "sync"

// guardia cuenta requests en vuelo por proveedor y registra el máximo
// observado. Es la aserción empírica de "no concurrencia por proveedor".
type guardia struct {
	mu          sync.Mutex
	limite      int
	enVuelo     map[string]int
	maxEnVuelo  map[string]int
	violaciones int
	requests    int
}

type estadisticas struct {
	MaxEnVuelo  map[string]int `json:"max_en_vuelo"`
	Violaciones int            `json:"violaciones"`
	Requests    int            `json:"requests"`
}

func nuevaGuardia(limite int) *guardia {
	return &guardia{limite: limite, enVuelo: map[string]int{}, maxEnVuelo: map[string]int{}}
}

func (g *guardia) entrar(proveedor string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests++
	if g.enVuelo[proveedor] >= g.limite {
		g.violaciones++
		return false
	}
	g.enVuelo[proveedor]++
	if g.enVuelo[proveedor] > g.maxEnVuelo[proveedor] {
		g.maxEnVuelo[proveedor] = g.enVuelo[proveedor]
	}
	return true
}

func (g *guardia) salir(proveedor string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.enVuelo[proveedor]--
}

func (g *guardia) stats() estadisticas {
	g.mu.Lock()
	defer g.mu.Unlock()
	copia := make(map[string]int, len(g.maxEnVuelo))
	for k, v := range g.maxEnVuelo {
		copia[k] = v
	}
	return estadisticas{MaxEnVuelo: copia, Violaciones: g.violaciones, Requests: g.requests}
}
```

- [ ] **Paso 4: `cmd/api-mock/main.go`**

```go
// api-mock simula la API interna (API Gateway + dos Lambdas): latencia
// configurable, errores con probabilidad configurable y una guardia que
// detecta si alguien la llama en paralelo para el mismo proveedor.
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	puerto       string
	latenciaBase time.Duration
	latenciaVar  time.Duration
	prob500      float64
	prob503      float64
	prob429      float64
	probTimeout  float64
	prob400      float64
	limite       int
}

func leerConfig() config {
	f := func(k string, def float64) float64 {
		if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
			return v
		}
		return def
	}
	ms := func(k string, def int) time.Duration {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
			return time.Duration(v) * time.Millisecond
		}
		return time.Duration(def) * time.Millisecond
	}
	puerto := os.Getenv("PUERTO")
	if puerto == "" {
		puerto = "8080"
	}
	return config{
		puerto:       puerto,
		latenciaBase: ms("LATENCIA_BASE_MS", 200),
		latenciaVar:  ms("LATENCIA_VAR_MS", 100),
		prob500:      f("PROB_500", 0),
		prob503:      f("PROB_503", 0),
		prob429:      f("PROB_429", 0),
		probTimeout:  f("PROB_TIMEOUT", 0),
		prob400:      f("PROB_400", 0),
		limite:       int(f("MAX_CONCURRENCIA_PROVEEDOR", 1)),
	}
}

func main() {
	cfg := leerConfig()
	g := nuevaGuardia(cfg.limite)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(g.stats())
	})

	mux.HandleFunc("POST /{proveedor}/{resto...}", func(w http.ResponseWriter, r *http.Request) {
		proveedor := r.PathValue("proveedor")
		if !g.entrar(proveedor) {
			slog.Error("violación de concurrencia", "proveedor", proveedor)
			http.Error(w, `{"error":"concurrencia por proveedor"}`, http.StatusConflict)
			return
		}
		defer g.salir(proveedor)

		// Simula las dos Lambdas: dos tramos de latencia.
		time.Sleep(latencia(cfg) / 2)
		time.Sleep(latencia(cfg) / 2)

		// Cabecera X-Forzar permite a los tests pedir un código concreto.
		if forzado := r.Header.Get("X-Forzar"); forzado != "" {
			responder(w, forzado, cfg)
			return
		}
		responder(w, sortear(cfg), cfg)
	})

	slog.Info("api-mock escuchando", "puerto", cfg.puerto, "limite", cfg.limite)
	if err := http.ListenAndServe(":"+cfg.puerto, mux); err != nil {
		slog.Error("servidor", "err", err)
		os.Exit(1)
	}
}

func latencia(cfg config) time.Duration {
	if cfg.latenciaVar == 0 {
		return cfg.latenciaBase
	}
	return cfg.latenciaBase + time.Duration(rand.Int64N(int64(cfg.latenciaVar)))
}

func sortear(cfg config) string {
	x := rand.Float64()
	acum := 0.0
	for _, c := range []struct {
		p    float64
		code string
	}{{cfg.prob500, "500"}, {cfg.prob503, "503"}, {cfg.prob429, "429"}, {cfg.probTimeout, "timeout"}, {cfg.prob400, "400"}} {
		acum += c.p
		if x < acum {
			return c.code
		}
	}
	return "200"
}

func responder(w http.ResponseWriter, code string, _ config) {
	w.Header().Set("Content-Type", "application/json")
	switch strings.ToLower(code) {
	case "timeout":
		time.Sleep(35 * time.Second) // mayor al timeout del cliente (30 s)
		w.WriteHeader(http.StatusGatewayTimeout)
	case "200":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"ts":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`))
	default:
		n, err := strconv.Atoi(code)
		if err != nil {
			n = 500
		}
		w.WriteHeader(n)
		_, _ = w.Write([]byte(`{"error":"simulado"}`))
	}
}
```

- [ ] **Paso 5: Correr unitarios y probar a mano**

```bash
go test ./cmd/api-mock/ -v
make infra && docker compose -f deploy/docker-compose.yml up -d --build api-mock
curl -s -X POST localhost:8080/acme/consulta -d '{}' ; echo
curl -s -X POST -H 'X-Forzar: 503' localhost:8080/acme/consulta -d '{}' ; echo
curl -s localhost:8080/stats ; echo
```

Esperado: `{"ok":true,...}`, `{"error":"simulado"}` con status 503, stats con `"acme":1` y `"violaciones":0`.

- [ ] **Paso 6: Commit**

```bash
git add cmd/api-mock
git commit -m "API mock con latencia, errores configurables y guardia de concurrencia por proveedor"
```

---

### Tarea 9: Binario del worker y prueba de serialización

**Archivos:**
- Crear: `cmd/worker/main.go` (reemplaza el stub), `internal/worker/loop.go`
- Test: `internal/worker/loop_test.go`, `tests/integracion/serializacion_test.go`

**Interfaces:**
- Consume: `Procesador`, `cola.SQS`, `resultados.Dynamo`, `apiclient.NuevoHTTP`, `awscfg.Cargar`.
- Produce:
  ```go
  // internal/worker
  func Correr(ctx context.Context, p *Procesador, concurrencia int) // bloquea hasta ctx.Done y drena los mensajes en vuelo
  ```
  Variables de entorno del worker: `COLA_URL`, `TABLA_RESULTADOS`, `API_BASE_URL`, `CONCURRENCIA`, `ESCALA_TIEMPO`.

- [ ] **Paso 1: Test del loop con fakes**

`internal/worker/loop_test.go`:

```go
package worker

import (
	"context"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
)

func TestCorrerProcesaTodoYTermina(t *testing.T) {
	c := cola.NuevaFake()
	r := resultados.NuevaFake()
	var msgs []dominio.Mensaje
	for i := int64(1); i <= 30; i++ {
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: "p" + string(rune('a'+i%5)), Fecha: "2026-11-08", Body: []byte(`{}`)})
	}
	_ = c.Enviar(context.Background(), msgs)
	p := &Procesador{Cola: c, Resultados: r, API: &apiclient.Fake{Programa: []apiclient.Respuesta{{Status: 200, Body: []byte(`{}`)}}},
		Ahora: time.Now, Rnd: func() float64 { return 0.5 }, Escala: 1}

	ctx, cancel := context.WithCancel(context.Background())
	hecho := make(chan struct{})
	go func() { Correr(ctx, p, 4); close(hecho) }()

	limite := time.Now().Add(5 * time.Second)
	for time.Now().Before(limite) && len(c.Borrados) < 30 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-hecho:
	case <-time.After(3 * time.Second):
		t.Fatal("Correr no terminó tras cancelar el contexto")
	}
	if len(r.Items) != 30 {
		t.Fatalf("procesados %d de 30", len(r.Items))
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./internal/worker/ -run TestCorrer
```

- [ ] **Paso 3: `internal/worker/loop.go`**

```go
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Correr recibe mensajes y los procesa con hasta `concurrencia` goroutines.
// Un ciclo de Recibir trae mensajes de proveedores distintos (garantía FIFO),
// así que procesarlos en paralelo no viola la serialización por proveedor.
// Al cancelar el contexto deja de recibir y espera a los que están en vuelo.
func Correr(ctx context.Context, p *Procesador, concurrencia int) {
	sem := make(chan struct{}, concurrencia)
	var wg sync.WaitGroup
	for ctx.Err() == nil {
		msgs, err := p.Cola.Recibir(ctx, concurrencia)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("recibir", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if len(msgs) == 0 {
			// El fake no bloquea; contra SQS el long polling ya esperó 20 s.
			select {
			case <-ctx.Done():
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		for _, m := range msgs {
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				// El procesamiento usa un contexto propio: una cancelación por
				// apagado no debe dejar un mensaje a medias.
				ctxMsg, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				if err := p.Procesar(ctxMsg, m); err != nil {
					slog.Error("procesar", "registro", m.RegistroID, "err", err)
				}
			}()
		}
	}
	wg.Wait()
}
```

- [ ] **Paso 4: `cmd/worker/main.go`**

```go
// worker consume la cola FIFO, llama a la API interna y guarda resultados.
package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/awscfg"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/resultados"
	"consultas-diarias/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		slog.Error("config aws", "err", err)
		os.Exit(1)
	}
	concurrencia, _ := strconv.Atoi(os.Getenv("CONCURRENCIA"))
	if concurrencia <= 0 {
		concurrencia = 10
	}
	escala, _ := strconv.ParseFloat(os.Getenv("ESCALA_TIEMPO"), 64)
	if escala < 1 {
		escala = 1
	}

	p := &worker.Procesador{
		Cola:       cola.NuevaSQS(sqs.NewFromConfig(cfg), os.Getenv("COLA_URL")),
		Resultados: resultados.NuevaDynamo(dynamodb.NewFromConfig(cfg), os.Getenv("TABLA_RESULTADOS")),
		API:        apiclient.NuevoHTTP(os.Getenv("API_BASE_URL"), dominio.TimeoutHTTP),
		Ahora:      time.Now,
		Rnd:        rand.Float64,
		Escala:     escala,
	}
	slog.Info("worker iniciado", "concurrencia", concurrencia, "escala", escala)
	worker.Correr(ctx, p, concurrencia)
	slog.Info("worker detenido")
}
```

- [ ] **Paso 5: Test de integración de la invariante central**

`tests/integracion/serializacion_test.go`:

```go
//go:build integration

package integracion

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/worker"
)

type stats struct {
	MaxEnVuelo  map[string]int `json:"max_en_vuelo"`
	Violaciones int            `json:"violaciones"`
}

func leerStats(t *testing.T, base string) stats {
	t.Helper()
	resp, err := http.Get(base + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s stats
	_ = json.NewDecoder(resp.Body).Decode(&s)
	return s
}

// Un proveedor, 20 mensajes, 5 workers con concurrencia 4 cada uno.
// La API mock responde 409 si recibe dos requests simultáneos del mismo
// proveedor; el test pasa sólo si eso nunca ocurre.
func TestNuncaDosRequestsAlMismoProveedor(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var msgs []dominio.Mensaje
	for i := int64(1); i <= 20; i++ {
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: "unico", Endpoint: "/c", Body: []byte(`{}`), Fecha: "2026-11-08"})
	}
	for i := int64(21); i <= 40; i++ {
		msgs = append(msgs, dominio.Mensaje{RegistroID: i, Proveedor: "otro", Endpoint: "/c", Body: []byte(`{}`), Fecha: "2026-11-08"})
	}
	if err := e.Cola.Enviar(ctx, msgs); err != nil {
		t.Fatal(err)
	}

	antes := leerStats(t, e.APIBase)
	for i := 0; i < 5; i++ {
		p := &worker.Procesador{Cola: e.Cola, Resultados: e.Resultados,
			API: apiclient.NuevoHTTP(e.APIBase, dominio.TimeoutHTTP), Ahora: time.Now, Rnd: rand.Float64, Escala: 1}
		go worker.Correr(ctx, p, 4)
	}

	esperar(t, 60*time.Second, func() bool {
		_, ok1, _ := e.Resultados.Leer(ctx, 20, "2026-11-08")
		_, ok2, _ := e.Resultados.Leer(ctx, 40, "2026-11-08")
		return ok1 && ok2
	})
	cancel()

	despues := leerStats(t, e.APIBase)
	if despues.Violaciones != antes.Violaciones {
		t.Fatalf("hubo %d violaciones de concurrencia", despues.Violaciones-antes.Violaciones)
	}
	if despues.MaxEnVuelo["unico"] > 1 || despues.MaxEnVuelo["otro"] > 1 {
		t.Fatalf("máximo en vuelo superó 1: %+v", despues.MaxEnVuelo)
	}
	for i := int64(1); i <= 40; i++ {
		r, ok, _ := e.Resultados.Leer(ctx, i, "2026-11-08")
		if !ok || r.Estado != dominio.EstadoOK {
			t.Fatalf("registro %d: %+v %v", i, r, ok)
		}
	}
}
```

El api-mock del compose tiene probabilidades de error distintas de cero. Para este test levantalo con errores en cero: en `deploy/docker-compose.yml` dejá los `PROB_*` en `"0"` por defecto y movelos a un archivo `deploy/demo.env` que se carga sólo en la demo (Tarea 13).

- [ ] **Paso 6: Correr**

```bash
go test ./internal/worker/ -v
make test-integration
```

Esperado: unitarios PASS; `TestNuncaDosRequestsAlMismoProveedor` PASS, con `violaciones` sin cambios y `max_en_vuelo` igual a 1 para ambos proveedores.

- [ ] **Paso 7: Commit**

```bash
git add cmd/worker internal/worker tests deploy/docker-compose.yml
git commit -m "Worker con loop de consumo, apagado ordenado y prueba de serialización por proveedor"
```

---

### Tarea 10: Scheduler: encolar una franja

**Archivos:**
- Crear: `internal/scheduler/franja.go`, `cmd/scheduler/main.go` (reemplaza el stub)
- Test: `internal/scheduler/franja_test.go`

**Interfaces:**
- Consume: `fuente.Fuente`, `cola.Cola`, `SlotActual`.
- Produce:
  ```go
  type Scheduler struct { Fuente fuente.Fuente; Cola cola.Cola }
  func (s *Scheduler) EncolarFranja(ctx context.Context, fecha string, slot int) (encolados int, err error)
  ```
  Variables de entorno del scheduler: `POSTGRES_URL`, `COLA_URL`, `ESCALA_TIEMPO`.

- [ ] **Paso 1: Test con fakes**

`internal/scheduler/franja_test.go`:

```go
package scheduler

import (
	"context"
	"testing"

	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/fuente"
)

func regs() []dominio.Registro {
	return []dominio.Registro{
		{ID: 1, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Slot: 5},
		{ID: 2, Proveedor: "a", Endpoint: "/x", Body: []byte(`{}`), Slot: 5},
		{ID: 3, Proveedor: "b", Endpoint: "/y", Body: []byte(`{}`), Slot: 5},
		{ID: 4, Proveedor: "c", Endpoint: "/z", Body: []byte(`{}`), Slot: 6},
	}
}

func TestEncolarFranjaPublicaLosDelSlot(t *testing.T) {
	f := fuente.NuevaFake(regs())
	c := cola.NuevaFake()
	s := &Scheduler{Fuente: f, Cola: c}
	n, err := s.EncolarFranja(context.Background(), "2026-11-08", 5)
	if err != nil || n != 3 || c.Enviados != 3 {
		t.Fatalf("n=%d enviados=%d err=%v", n, c.Enviados, err)
	}
	if f.Franjas["2026-11-08:5"] != 3 {
		t.Fatalf("la franja debía cerrarse con 3: %v", f.Franjas)
	}
	got, _ := c.Recibir(context.Background(), 10)
	if got[0].Fecha != "2026-11-08" || got[0].Endpoint != "/x" {
		t.Fatalf("mensaje mal armado: %+v", got[0])
	}
}

func TestEncolarFranjaEsIdempotente(t *testing.T) {
	f := fuente.NuevaFake(regs())
	c := cola.NuevaFake()
	s := &Scheduler{Fuente: f, Cola: c}
	_, _ = s.EncolarFranja(context.Background(), "2026-11-08", 5)
	n, err := s.EncolarFranja(context.Background(), "2026-11-08", 5)
	if err != nil || n != 0 || c.Enviados != 3 {
		t.Fatalf("segunda ejecución no debía encolar: n=%d enviados=%d err=%v", n, c.Enviados, err)
	}
}

func TestEncolarFranjaSaltaProveedoresAtrasados(t *testing.T) {
	f := fuente.NuevaFake(regs())
	f.Atrasados = []string{"a"}
	c := cola.NuevaFake()
	s := &Scheduler{Fuente: f, Cola: c}
	n, _ := s.EncolarFranja(context.Background(), "2026-11-08", 5)
	if n != 1 || c.Enviados != 1 {
		t.Fatalf("sólo 'b' debía encolarse: n=%d", n)
	}
	got, _ := c.Recibir(context.Background(), 10)
	if got[0].Proveedor != "b" {
		t.Fatalf("got %+v", got[0])
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./internal/scheduler/ -run TestEncolar
```

- [ ] **Paso 3: `internal/scheduler/franja.go`**

```go
package scheduler

import (
	"context"
	"fmt"
	"log/slog"

	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/fuente"
)

type Scheduler struct {
	Fuente fuente.Fuente
	Cola   cola.Cola
}

// EncolarFranja publica los registros de la franja. Es idempotente por
// (fecha, slot) gracias a corridas_diarias, y respeta la política de
// medianoche: un proveedor con pendientes del día anterior no se encola.
func (s *Scheduler) EncolarFranja(ctx context.Context, fecha string, slot int) (int, error) {
	nueva, err := s.Fuente.IniciarFranja(ctx, fecha, slot)
	if err != nil {
		return 0, err
	}
	if !nueva {
		slog.Info("franja ya publicada", "fecha", fecha, "slot", slot)
		return 0, nil
	}

	atrasados, err := s.Fuente.ProveedoresAtrasados(ctx)
	if err != nil {
		return 0, err
	}
	saltar := make(map[string]bool, len(atrasados))
	for _, p := range atrasados {
		saltar[p] = true
	}

	regs, err := s.Fuente.RegistrosDeSlot(ctx, slot)
	if err != nil {
		return 0, err
	}
	msgs := make([]dominio.Mensaje, 0, len(regs))
	for _, r := range regs {
		if saltar[r.Proveedor] {
			continue
		}
		msgs = append(msgs, dominio.Mensaje{
			RegistroID: r.ID, Proveedor: r.Proveedor, Endpoint: r.Endpoint, Body: r.Body, Fecha: fecha,
		})
	}
	if len(msgs) > 0 {
		if err := s.Cola.Enviar(ctx, msgs); err != nil {
			return 0, fmt.Errorf("encolar franja %s/%d: %w", fecha, slot, err)
		}
	}
	if err := s.Fuente.CerrarFranja(ctx, fecha, slot, len(msgs)); err != nil {
		return len(msgs), err
	}
	slog.Info("franja encolada", "fecha", fecha, "slot", slot, "registros", len(regs), "encolados", len(msgs), "saltados", len(regs)-len(msgs))
	return len(msgs), nil
}
```

Si `Enviar` falla a mitad, la franja queda en `encolando` y los mensajes ya enviados tienen `MessageDeduplicationId`, así que una reejecución manual (borrar la fila de `corridas_diarias`) no duplica dentro de la ventana de 5 minutos. Fuera de la ventana, la escritura condicional en DynamoDB absorbe el duplicado. Esto es exactamente la discusión de idempotencia en tres capas de la spec.

- [ ] **Paso 4: `cmd/scheduler/main.go`**

Un ticker reemplaza a EventBridge. En AWS este `main` se reduce a un handler de Lambda que llama a `EncolarFranja` con el slot del momento.

```go
// scheduler publica en la cola los registros de la franja actual.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"consultas-diarias/internal/awscfg"
	"consultas-diarias/internal/cola"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/fuente"
	"consultas-diarias/internal/scheduler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		slog.Error("config aws", "err", err)
		os.Exit(1)
	}
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	escala, _ := strconv.ParseFloat(os.Getenv("ESCALA_TIEMPO"), 64)
	if escala < 1 {
		escala = 1
	}
	s := &scheduler.Scheduler{
		Fuente: fuente.NuevaPostgres(pool),
		Cola:   cola.NuevaSQS(sqs.NewFromConfig(cfg), os.Getenv("COLA_URL")),
	}

	duracionFranja := time.Duration(float64(24*time.Hour/dominio.FranjasPorDia) / escala)
	inicio := time.Now()
	slog.Info("scheduler iniciado", "escala", escala, "franja", duracionFranja)

	// Encola la franja actual de inmediato y luego una por tick. Si una
	// franja se saltea (proceso caído), la siguiente ejecución la detecta
	// porque SlotActual avanzó y la fila de corridas_diarias no existe:
	// recorremos desde la última publicada hasta la actual.
	ultimo := -1
	tick := time.NewTicker(duracionFranja)
	defer tick.Stop()
	for {
		slot, fecha := scheduler.SlotActual(time.Now(), inicio, escala)
		if slot != ultimo {
			if _, err := s.EncolarFranja(ctx, fecha, slot); err != nil {
				slog.Error("encolar", "slot", slot, "err", err)
			}
			ultimo = slot
		}
		select {
		case <-ctx.Done():
			slog.Info("scheduler detenido")
			return
		case <-tick.C:
		}
	}
}
```

- [ ] **Paso 5: Correr unitarios y una pasada manual**

```bash
go test ./internal/scheduler/ -v
make infra
docker compose -f deploy/docker-compose.yml exec postgres psql -U app -d consultas -c \
  "INSERT INTO registros (proveedor, endpoint, body, slot) SELECT 'p'||(i%3), '/c', '{}', (now()::time - '00:00'::time)::interval / interval '5 min' FROM generate_series(1,6) i"
ESCALA_TIEMPO=1 COLA_URL=http://localhost:4566/000000000000/consultas.fifo \
  POSTGRES_URL=postgres://app:app@localhost:5432/consultas \
  AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  timeout 5 go run ./cmd/scheduler
docker compose -f deploy/docker-compose.yml exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/consultas.fifo --attribute-names ApproximateNumberOfMessages
```

Esperado: log `franja encolada ... encolados=6` y la cola con 6 mensajes.

- [ ] **Paso 6: Commit**

```bash
git add cmd/scheduler internal/scheduler
git commit -m "Scheduler que encola la franja actual de forma idempotente"
```

---

### Tarea 11: Seed con distribución sesgada

**Archivos:**
- Crear: `cmd/seed/main.go` (reemplaza el stub), `cmd/seed/pareto.go`
- Test: `cmd/seed/pareto_test.go`

**Interfaces:**
- Consume: la consulta SQL de asignación de slots (spec 5.2).
- Produce: tabla `registros` con `REGISTROS` filas repartidas en `PROVEEDORES` proveedores siguiendo una ley de potencias, con `slot` calculado. Variables: `POSTGRES_URL`, `REGISTROS` (default 100000), `PROVEEDORES` (default 200), `SEMILLA` (default 1).

- [ ] **Paso 1: Test del reparto**

`cmd/seed/pareto_test.go`:

```go
package main

import "testing"

func TestRepartoParetoSumaYSesgo(t *testing.T) {
	r := repartoPareto(100000, 200, 1.1)
	total := 0
	for _, n := range r {
		total += n
	}
	if total != 100000 {
		t.Fatalf("suma %d", total)
	}
	top := 0
	for i := 0; i < 40; i++ { // 20 % de los proveedores
		top += r[i]
	}
	if top < 60000 {
		t.Fatalf("el 20 %% de los proveedores debía concentrar al menos 60 %% de registros, tiene %d", top)
	}
	for i, n := range r {
		if n < 1 {
			t.Fatalf("proveedor %d sin registros", i)
		}
	}
}
```

- [ ] **Paso 2: Correr y ver que falla**

```bash
go test ./cmd/seed/
```

- [ ] **Paso 3: `cmd/seed/pareto.go`**

```go
package main

import "math"

// repartoPareto devuelve cuántos registros recibe cada proveedor siguiendo
// una ley de potencias (Zipf con exponente s): el proveedor i recibe una
// porción proporcional a 1/(i+1)^s. Garantiza al menos 1 por proveedor y
// que la suma sea exactamente total.
func repartoPareto(total, proveedores int, s float64) []int {
	pesos := make([]float64, proveedores)
	suma := 0.0
	for i := range pesos {
		pesos[i] = 1 / math.Pow(float64(i+1), s)
		suma += pesos[i]
	}
	out := make([]int, proveedores)
	asignado := 0
	for i := range out {
		out[i] = max(1, int(float64(total)*pesos[i]/suma))
		asignado += out[i]
	}
	// Ajusta el redondeo sobre el proveedor más grande.
	out[0] += total - asignado
	return out
}
```

- [ ] **Paso 4: `cmd/seed/main.go`**

```go
// seed carga un dataset sintético en la tabla registros y calcula los slots.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const sqlSlots = `
UPDATE registros r SET slot = s.slot
FROM (
  SELECT id,
         (ROW_NUMBER() OVER (PARTITION BY proveedor ORDER BY id) - 1) * 288
           / COUNT(*) OVER (PARTITION BY proveedor) AS slot
  FROM registros
  WHERE activo
) s
WHERE r.id = s.id`

func entero(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	total := entero("REGISTROS", 100000)
	proveedores := entero("PROVEEDORES", 200)

	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, `TRUNCATE registros, corridas_diarias, proveedores_estado`); err != nil {
		slog.Error("truncate", "err", err)
		os.Exit(1)
	}

	reparto := repartoPareto(total, proveedores, 1.1)
	filas := make([][]any, 0, total)
	for i, n := range reparto {
		prov := fmt.Sprintf("proveedor-%03d", i)
		for j := 0; j < n; j++ {
			filas = append(filas, []any{prov, "/consulta", fmt.Sprintf(`{"cuenta":"%s-%d"}`, prov, j)})
		}
	}

	inicio := time.Now()
	n, err := pool.CopyFrom(ctx, pgx.Identifier{"registros"}, []string{"proveedor", "endpoint", "body"}, pgx.CopyFromRows(filas))
	if err != nil {
		slog.Error("copy", "err", err)
		os.Exit(1)
	}
	slog.Info("registros cargados", "n", n, "en", time.Since(inicio))

	inicio = time.Now()
	if _, err := pool.Exec(ctx, sqlSlots); err != nil {
		slog.Error("slots", "err", err)
		os.Exit(1)
	}
	slog.Info("slots asignados", "en", time.Since(inicio))

	var maxProv string
	var maxN int
	_ = pool.QueryRow(ctx, `SELECT proveedor, count(*) FROM registros GROUP BY 1 ORDER BY 2 DESC LIMIT 1`).Scan(&maxProv, &maxN)
	slog.Info("proveedor más cargado", "proveedor", maxProv, "registros", maxN, "por_franja", maxN/288)
}
```

- [ ] **Paso 5: Correr**

```bash
go test ./cmd/seed/ -v
make infra && make seed
docker compose -f deploy/docker-compose.yml exec postgres psql -U app -d consultas -c \
  "SELECT slot, count(*) FROM registros GROUP BY slot ORDER BY count(*) DESC LIMIT 3" -c \
  "SELECT slot, count(*) FROM registros GROUP BY slot ORDER BY count(*) ASC LIMIT 3"
```

Esperado: 100.000 filas, log del proveedor más cargado (~9.000 registros, ~31 por franja), y la cantidad por franja entre ~340 y ~355 en todos los slots: la suma de proveedores uniformes es uniforme.

- [ ] **Paso 6: Commit**

```bash
git add cmd/seed
git commit -m "Seed con distribución Pareto de proveedores y cálculo de franjas"
```

---

### Tarea 12: Pruebas de integración de reintentos y DLQ

**Archivos:**
- Modificar: `cmd/api-mock/main.go` (el handler ya soporta `X-Forzar`; se agrega `X-Forzar-Secuencia`)
- Modificar: `internal/apiclient/http.go` (propaga cabeceras del body de prueba)
- Test: `tests/integracion/reintentos_test.go`

**Interfaces:**
- Produce: el api-mock acepta `X-Forzar-Secuencia: 503,503,200` y responde esa secuencia, por proveedor, en llamadas sucesivas. El cliente HTTP lee un campo opcional `_forzar_secuencia` del body JSON y lo manda como cabecera; sólo lo usan los tests.

- [ ] **Paso 1: Secuencias en el api-mock**

En `cmd/api-mock/main.go`, agregar al `struct guardia` un mapa de secuencias y un método, y usarlo en el handler antes de `sortear`:

```go
// en guardia.go
func (g *guardia) siguienteForzado(proveedor, secuencia string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.secuencias == nil {
		g.secuencias = map[string][]string{}
	}
	if _, ok := g.secuencias[proveedor]; !ok {
		g.secuencias[proveedor] = strings.Split(secuencia, ",")
	}
	cola := g.secuencias[proveedor]
	if len(cola) == 0 {
		return "", false
	}
	g.secuencias[proveedor] = cola[1:]
	return strings.TrimSpace(cola[0]), true
}
```

con `secuencias map[string][]string` como campo nuevo de `guardia` e `import "strings"`. En el handler:

```go
		if seq := r.Header.Get("X-Forzar-Secuencia"); seq != "" {
			if code, ok := g.siguienteForzado(proveedor, seq); ok {
				responder(w, code, cfg)
				return
			}
		}
```

- [ ] **Paso 2: Propagar desde el cliente**

En `internal/apiclient/http.go`, antes de `h.cliente.Do(req)`:

```go
	var sonda struct {
		Secuencia string `json:"_forzar_secuencia"`
	}
	if json.Unmarshal(body, &sonda) == nil && sonda.Secuencia != "" {
		req.Header.Set("X-Forzar-Secuencia", sonda.Secuencia)
	}
```

e `import "encoding/json"`. Es un gancho de prueba explícito, documentado en el código; la API real lo ignora.

- [ ] **Paso 3: Tests**

`tests/integracion/reintentos_test.go`:

```go
//go:build integration

package integracion

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"consultas-diarias/internal/apiclient"
	"consultas-diarias/internal/dominio"
	"consultas-diarias/internal/worker"
)

func lanzarWorker(ctx context.Context, e *entorno, escala float64) {
	p := &worker.Procesador{Cola: e.Cola, Resultados: e.Resultados,
		API: apiclient.NuevoHTTP(e.APIBase, dominio.TimeoutHTTP), Ahora: time.Now, Rnd: rand.Float64, Escala: escala}
	go worker.Correr(ctx, p, 2)
}

func enviarUno(t *testing.T, e *entorno, id int64, proveedor, secuencia string) {
	t.Helper()
	body := `{"_forzar_secuencia":"` + secuencia + `"}`
	if err := e.Cola.Enviar(context.Background(), []dominio.Mensaje{
		{RegistroID: id, Proveedor: proveedor, Endpoint: "/c", Body: []byte(body), Fecha: "2026-11-08"},
	}); err != nil {
		t.Fatal(err)
	}
}

// Escala 600: el backoff de 30 s pasa a 50 ms, el de 2 min a 200 ms.
func TestReintentoExitoso(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	enviarUno(t, e, 101, "reint-ok", "503,500,200")
	lanzarWorker(ctx, e, 600)
	esperar(t, 30*time.Second, func() bool {
		_, ok, _ := e.Resultados.Leer(ctx, 101, "2026-11-08")
		return ok
	})
	r, _, _ := e.Resultados.Leer(ctx, 101, "2026-11-08")
	if r.Estado != dominio.EstadoOK || r.Intentos != 3 || r.HTTPStatus != 200 {
		t.Fatalf("resultado: %+v", r)
	}
}

func TestNoReintentableEnUnaPasada(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	enviarUno(t, e, 102, "fallo-400", "400")
	lanzarWorker(ctx, e, 600)
	esperar(t, 30*time.Second, func() bool {
		_, ok, _ := e.Resultados.Leer(ctx, 102, "2026-11-08")
		return ok
	})
	r, _, _ := e.Resultados.Leer(ctx, 102, "2026-11-08")
	if r.Estado != dominio.EstadoFalloDefinitivo || r.Intentos != 1 || r.HTTPStatus != 400 {
		t.Fatalf("resultado: %+v", r)
	}
}

func TestAgotadoTerminaEnDLQ(t *testing.T) {
	e := armarEntorno(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	enviarUno(t, e, 103, "siempre-503", "503,503,503,503,503,503")
	lanzarWorker(ctx, e, 600)
	esperar(t, 60*time.Second, func() bool {
		r, ok, _ := e.Resultados.Leer(ctx, 103, "2026-11-08")
		return ok && r.Estado == dominio.EstadoAgotado
	})
	r, _, _ := e.Resultados.Leer(ctx, 103, "2026-11-08")
	if r.Intentos != dominio.IntentosMax {
		t.Fatalf("intentos = %d", r.Intentos)
	}
	// Tras el quinto intento el mensaje queda en vuelo hasta que vence el
	// visibility timeout (60 s); recién entonces SQS lo mueve a la DLQ.
	esperar(t, 90*time.Second, func() bool {
		out, err := e.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(e.DLQURL),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
		})
		return err == nil && out.Attributes["ApproximateNumberOfMessages"] == "1"
	})
}
```

El último test tarda hasta 90 s por el visibility timeout real. Para acelerar la suite, la cola del compose puede crearse con `VisibilityTimeout=10` y el worker con `escala` suficiente para que los backoffs sean menores; en producción el valor es 60 s. Si optás por eso, cambialo en `deploy/localstack-init.sh` y documentalo en el README.

- [ ] **Paso 4: Correr**

```bash
go test ./cmd/api-mock/ ./internal/apiclient/ && make test-integration
```

Esperado: todos los tests de integración PASS.

- [ ] **Paso 5: Commit**

```bash
git add cmd/api-mock internal/apiclient tests
git commit -m "Pruebas de integración de reintentos, fallos definitivos y DLQ"
```

---

### Tarea 13: Demo de carga con día comprimido

**Archivos:**
- Crear: `deploy/demo.env`, `cmd/informe/main.go`
- Modificar: `Makefile` (target `demo`), `deploy/docker-compose.yml` (`env_file` para api-mock)

**Interfaces:**
- Produce: `make demo` corre un día completo comprimido con 100.000 registros y 10 workers, y `cmd/informe` imprime una tabla con: total procesados por estado, latencia p50/p95, los 5 proveedores con mayor saturación estimada, mensajes en DLQ y mensajes todavía pendientes.

- [ ] **Paso 1: `deploy/demo.env`**

```
LATENCIA_BASE_MS=150
LATENCIA_VAR_MS=100
PROB_500=0.02
PROB_503=0.02
PROB_429=0.01
PROB_TIMEOUT=0
PROB_400=0.01
MAX_CONCURRENCIA_PROVEEDOR=1
```

`PROB_TIMEOUT` en 0 porque un timeout real de 30 s en tiempo comprimido distorsiona la demo; se prueba en la Tarea 12.

- [ ] **Paso 2: Compose**

En `deploy/docker-compose.yml`, al servicio `api-mock` agregarle `env_file: [demo.env]` sólo cuando `DEMO=1`. Como compose no condiciona `env_file`, usá una variable: en `environment` del api-mock reemplazá los `PROB_*` fijos por `PROB_500: ${PROB_500:-0}` y así con cada uno; `make demo` exporta las variables leyendo `demo.env`. Y en `worker` y `scheduler`: `ESCALA_TIEMPO: ${ESCALA_TIEMPO:-1}`.

- [ ] **Paso 3: `cmd/informe/main.go`**

```go
// informe resume el estado de una corrida: resultados por estado, latencias,
// saturación estimada por proveedor y mensajes pendientes.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"consultas-diarias/internal/awscfg"
)

type fila struct {
	Proveedor  string `dynamodbav:"proveedor"`
	Estado     string `dynamodbav:"estado"`
	LatenciaMs int64  `dynamodbav:"latencia_ms"`
	Intentos   int    `dynamodbav:"intentos"`
}

func main() {
	ctx := context.Background()
	cfg, err := awscfg.Cargar(ctx)
	if err != nil {
		panic(err)
	}
	ddb := dynamodb.NewFromConfig(cfg)
	var filas []fila
	pag := dynamodb.NewScanPaginator(ddb, &dynamodb.ScanInput{
		TableName:            aws.String(os.Getenv("TABLA_RESULTADOS")),
		ProjectionExpression: aws.String("proveedor, estado, latencia_ms, intentos"),
	})
	for pag.HasMorePages() {
		out, err := pag.NextPage(ctx)
		if err != nil {
			panic(err)
		}
		var parte []fila
		if err := attributevalue.UnmarshalListOfMaps(out.Items, &parte); err != nil {
			panic(err)
		}
		filas = append(filas, parte...)
	}

	porEstado := map[string]int{}
	var lat []int64
	type acum struct {
		n     int
		sumMs int64
	}
	porProv := map[string]*acum{}
	for _, f := range filas {
		porEstado[f.Estado]++
		lat = append(lat, f.LatenciaMs)
		a := porProv[f.Proveedor]
		if a == nil {
			a = &acum{}
			porProv[f.Proveedor] = a
		}
		a.n++
		a.sumMs += f.LatenciaMs * int64(f.Intentos) // latencia efectiva incluyendo reintentos
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) int64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)]
	}

	fmt.Printf("Resultados: %d\n", len(filas))
	for _, e := range []string{"ok", "fallo_definitivo", "agotado"} {
		fmt.Printf("  %-18s %d\n", e, porEstado[e])
	}
	fmt.Printf("Latencia p50 %d ms, p95 %d ms\n", pct(0.5), pct(0.95))

	type sat struct {
		prov string
		k    int
		lMs  int64
		s    float64
	}
	var sats []sat
	for p, a := range porProv {
		l := a.sumMs / int64(a.n)
		// saturacion = K * L / 86400 s, con L la latencia efectiva media.
		sats = append(sats, sat{p, a.n, l, float64(a.n) * float64(l) / 1000 / 86400})
	}
	sort.Slice(sats, func(i, j int) bool { return sats[i].s > sats[j].s })
	fmt.Println("Proveedores con mayor saturación estimada (K × L / 86400):")
	for i := 0; i < len(sats) && i < 5; i++ {
		fmt.Printf("  %-16s K=%-6d L=%-5d ms  saturación=%.3f\n", sats[i].prov, sats[i].k, sats[i].lMs, sats[i].s)
	}

	sq := sqs.NewFromConfig(cfg)
	for _, nombre := range []string{"consultas.fifo", "consultas-dlq.fifo"} {
		url := os.Getenv("AWS_ENDPOINT_URL") + "/000000000000/" + nombre
		out, err := sq.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%-20s visibles=%s en_vuelo=%s\n", nombre,
			out.Attributes["ApproximateNumberOfMessages"], out.Attributes["ApproximateNumberOfMessagesNotVisible"])
	}
}
```

- [ ] **Paso 4: Target `demo` en el Makefile**

```makefile
demo:
	set -a; . deploy/demo.env; set +a; \
	ESCALA_TIEMPO=150 $(COMPOSE) up -d --build --scale worker=10 postgres localstack api-mock worker scheduler
	$(COMPOSE) run --rm seed
	@echo "Día comprimido en curso (~10 min). Seguilo con 'make logs'. Al terminar: make informe"

informe:
	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test TABLA_RESULTADOS=resultados \
	go run ./cmd/informe
	curl -s localhost:8080/stats | head -c 400; echo
```

Con `ESCALA_TIEMPO=150`, una franja dura 2 s y el día completo 9,6 minutos. El seed debe correr antes de que el scheduler publique la primera franja; como el scheduler arranca con la tabla vacía y encola 0 registros en el slot 0, el seed inmediato alcanza. Si querés exactitud, levantá `scheduler` después del seed: dividí el target en dos `up`.

- [ ] **Paso 5: Correr la demo y registrar resultados**

```bash
make down && make demo
# esperar ~10 minutos
make informe
```

Esperado: ~100.000 resultados; `ok` alrededor de 94 %, `fallo_definitivo` ~1 % (los 400), `agotado` cercano a 0 (cinco 5xx seguidos con probabilidad 4 % es 1e-7); `violaciones: 0` en las stats del mock; `max_en_vuelo` igual a 1 en todos los proveedores; los proveedores con mayor saturación son los `proveedor-00x` con K más alto. Guardá la salida en `docs/demo-resultados.md` tal cual.

- [ ] **Paso 6: Commit**

```bash
git add deploy Makefile cmd/informe docs/demo-resultados.md
git commit -m "Demo de carga con día comprimido e informe de saturación"
```

---

### Tarea 14: README

**Archivos:**
- Crear: `README.md`

**Interfaces:**
- Consume: la spec y `docs/demo-resultados.md`.

- [ ] **Paso 1: Escribir `README.md`** con estas secciones, en este orden, sin relleno:

1. **Qué es**: dos párrafos con el problema y la respuesta en una frase (SQS FIFO por proveedor + scheduler por franjas + workers Fargate).
2. **Arquitectura**: el diagrama de componentes de la spec (sección 4.1) y el de secuencia (4.4), copiados tal cual en Mermaid, con un párrafo de lectura debajo de cada uno.
3. **Decisiones clave**: tabla de cuatro columnas (decisión, por qué, alternativa descartada, costo de la elección) con las seis decisiones de la sección 8 de la spec, una fila cada una, máximo dos líneas por celda.
4. **El límite que no se puede diseñar**: la fórmula de saturación y la política de medianoche (spec 5.3 y 5.4), en media página. Es lo primero que preguntan.
5. **Cómo correr la POC**: requisitos (Docker, Go 1.23), `make infra`, `make test`, `make test-integration`, `make demo`, `make informe`. Qué espera ver quien lo corre.
6. **Mapeo POC → producción**: la tabla de la spec 11.1.
7. **Resultados de la demo**: contenido de `docs/demo-resultados.md` con dos líneas de interpretación.
8. **Qué quedó fuera** (spec 11.5) y qué haría distinto con más tiempo.
9. **Ciclo de vida y plan**: enlace a las secciones 13 y 14 de la spec y a este plan.

- [ ] **Paso 2: Revisar enlaces y renderizado de Mermaid**

```bash
grep -n '](docs/' README.md
```

Verificar que cada ruta exista. Abrir el README en el visor de GitHub o en el editor con vista previa de Mermaid.

- [ ] **Paso 3: Commit**

```bash
git add README.md
git commit -m "README con arquitectura, decisiones, ejecución y resultados de la demo"
```

---

## Autorrevisión del plan

**Cobertura de la spec.** Serialización por proveedor: Tareas 4, 7, 9. Distribución horaria y slots: 6, 11. Política de medianoche: 10. Clasificación y backoff: 2, 3, 5. Idempotencia en tres capas: 7 (dedup id y PutItem condicional), 10 (corridas_diarias). DLQ: 12. Métrica de saturación: 13. Modelo de datos: 1, 7. API mock con guardia: 8. Tiempo comprimido: 6, 9, 13. Documentación: 14. Sin tareas: autoscaling, alarmas, S3, IaC, job nocturno (fuera de alcance declarado en la spec 11.5; en la POC la saturación la calcula `informe` al final de la corrida).

**Consistencia de tipos.** `Procesador` tiene los mismos campos en 5, 9 y 12. `cola.Fake` expone `Enviados`, `Borrados`, `Pendientes()` y se usa así en 5 y 10. `resultados.Fake.Items` y `FallarN` en 4 y 5. `Resultados.Guardar` devuelve `ErrYaExiste` en interfaz, fake y Dynamo. `fuente.Fake.Franjas` guarda `-1` al iniciar y el conteo al cerrar, como lo lee el test de 10. `Dynamo.Leer` se define en 7 y se usa en 9 y 12. `SlotActual(t, inicio, escala)` en 6 y 10.
