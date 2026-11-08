COMPOSE = docker compose -f deploy/docker-compose.yml

.PHONY: up down infra test test-integration seed demo informe logs

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

demo:
	set -a; . deploy/demo.env; set +a; \
	export ESCALA_TIEMPO=150; \
	$(COMPOSE) up -d --build postgres localstack api-mock; \
	$(COMPOSE) run --rm seed; \
	$(COMPOSE) up -d --scale worker=10 worker scheduler
	@echo "Día comprimido en curso (~10 min). Seguilo con 'make logs'. Al terminar: make informe"

informe:
	AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test TABLA_RESULTADOS=resultados \
	go run ./cmd/informe
	curl -s localhost:8080/stats | head -c 400; echo
