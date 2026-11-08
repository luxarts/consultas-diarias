COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: up down infra test test-integration seed demo informe logs

# --wait espera los healthchecks: postgres listo y localstack con colas y tabla creadas.
infra:
	$(COMPOSE) up -d --build --wait postgres localstack api-mock

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

# Parte siempre de cero (borra volúmenes), reconstruye las imágenes y corta
# en el primer paso que falle.
demo:
	set -a; . deploy/demo.env; set +a; export ESCALA_TIEMPO=150; \
	$(COMPOSE) down -v && \
	$(COMPOSE) --profile seed build && \
	$(COMPOSE) up -d --wait postgres localstack api-mock && \
	$(COMPOSE) run --rm seed && \
	$(COMPOSE) up -d --scale worker=10 worker scheduler
	@echo "Día comprimido en curso (~10 min). Seguilo con 'make logs'; el scheduler termina con 'día comprimido completo'. Después: make informe"

# ESCALA_TIEMPO debe coincidir con la de la demo. FECHA (opcional) elige el día.
informe:
	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test TABLA_RESULTADOS=resultados \
	POSTGRES_URL=postgres://app:app@localhost:5432/consultas \
	API_BASE_URL=http://localhost:8080 ESCALA_TIEMPO=150 \
	go run ./cmd/informe
