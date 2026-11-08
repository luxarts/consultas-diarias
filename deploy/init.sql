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
