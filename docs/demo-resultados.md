# Resultados de la demo

Corrida del 8 de noviembre de 2026 con `make demo` (día comprimido 150×) sobre
Docker Desktop en una laptop: 100.000 registros, 200 proveedores con
distribución Zipf, 10 workers con 10 goroutines cada uno, LocalStack 3.8.1 para
SQS y DynamoDB, Postgres 16 y el mock de la API con los parámetros de
`deploy/demo.env` (latencia 2 a 4 ms, 2 % de 500, 2 % de 503, 1 % de 429, 1 %
de 400).

## Qué pasó

| Hito | Hora (UTC) |
|---|---|
| Scheduler arranca, publica la franja 0 | 19:49:23 |
| Franja 287 publicada, `día comprimido completo` | 20:01:55 |
| Informe tomado (cola todavía drenando) | 20:21 |

El día comprimido dura nominalmente 576 s (288 franjas de 2 s) y tardó 12 min
32 s: publicar una franja de 350 mensajes en LocalStack tarda más de 2 s, así que
el scheduler fue quedando detrás del reloj y el catch-up dentro del proceso fue
publicando las franjas atrasadas en orden. Se publicaron las 288, ninguna se
salteó y ninguna quedó en `encolando`.

Durante toda la corrida el mock no registró **ninguna violación de
concurrencia**: el máximo de requests simultáneos de un mismo proveedor fue 1,
con hasta 22 requests simultáneos sumando proveedores distintos.

Los errores se comportaron como estaba previsto:

- `fallo_definitivo`: 843 sobre 81.229 resultados (1,04 %), que es la
  probabilidad de 400 configurada. Ninguno se reintentó.
- Reintentables: el mock devolvió 5 % de 429/5xx y LocalStack agregó 1.662
  errores 500 propios en `PutItem` ("timed out waiting for a lock", un límite
  del emulador de DynamoDB bajo escritura concurrente). Los workers los trataron
  igual: backoff y nuevo intento. La diferencia entre requests al mock (85.862)
  y resultados (81.229) son esos reintentos.
- `agotado`: 0. Con 5 % de error por request, la probabilidad de cinco fallos
  seguidos es 3 × 10⁻⁷; la DLQ quedó vacía, como corresponde.

## Lo que no terminó, y por qué

Al momento del informe quedaban 18.771 consultas pendientes, todas de los dos
proveedores más grandes (`proveedor-000` con 21.383 registros y `proveedor-001`
con 9.928). Los otros 198 proveedores, incluido el tercero con 6.355 registros,
terminaron el día completos.

Es el límite de serialización del problema, visto en vivo. Cada proveedor
tiene un solo mensaje en vuelo, así que su ritmo máximo es un mensaje por ciclo
completo: recibir de la cola, llamar a la API, guardar en DynamoDB, borrar de la
cola. Contra LocalStack ese ciclo mide entre 200 y 500 ms, y en el día comprimido
`proveedor-000` necesitaría 74 consultas por franja de 2 s, es decir un ciclo de
27 ms. Llevado a tiempo real con la escala, un ciclo de 300 ms equivale a 45 s
por consulta; ningún diseño que respete "una a la vez" procesa 21.000 consultas
diarias de un proveedor a 45 s cada una. En producción el ciclo lo domina la
latencia de la API (cientos de ms) y el resto suma 20 a 40 ms, de modo que el
techo real por proveedor es el que calcula la spec: 86.400 / L consultas por
día.

Dos cosas que este caso muestra sobre el diseño:

- **La métrica de saturación `s` usa sólo la latencia de la API.** Por eso dice
  0,18 para un proveedor que no converge. En producción esa aproximación es
  razonable; en la demo la compresión infla justo la parte que `s` no mide. La
  señal que sí lo detecta es la que la spec usa para la política de medianoche:
  pendientes del día anterior al cambiar de fecha. El informe los imprime por
  proveedor, y en producción la alarma debería basarse en la antigüedad del
  mensaje más viejo de cada grupo además de en `s`.
- **La política de medianoche habría actuado.** Al día siguiente el job nocturno
  marcaría a `proveedor-000` y `proveedor-001` con `pendientes_ayer > 0`, el
  scheduler no les encolaría registros nuevos hasta que terminen el día
  anterior, y la deuda no crecería sin límite.

Los dos proveedores siguen drenando a unos dos o tres mensajes por segundo
entre ambos mientras los workers estén arriba; no hay nada trabado, sólo lento.
Con 1 millón de registros y la misma distribución el proveedor mayor tendría
unos 210.000 registros diarios, muy por encima del techo de 86.400 con L = 1 s:
es el caso que la spec identifica como irresoluble sin cambiar el requisito (o
negociar con el proveedor más de un request simultáneo, que es un cambio de un
parámetro en el mock y de ningún código en el worker).

## Salida de `make informe`

```
Fecha: 2026-11-08 (escala de tiempo 150)

Resultados: 81229
  ok                 80386
  fallo_definitivo   843
  agotado            0
Latencia medida p50 4 ms, p95 5 ms

Saturación por proveedor, s = K × L × escala / 86400 (L = latencia × intentos, K de Postgres)
Proveedores: 200, con s > 0.8: 0, con pendientes: 2, pendientes totales: 18771
  proveedor              K resultados pendientes     L ms      s
  proveedor-000      21383       6772      14611      4.9   0.18
  proveedor-001       9928       5768       4160      5.0   0.09
  proveedor-002       6355       6355          0      5.0   0.06
  proveedor-003       4631       4631          0      5.2   0.04
  proveedor-004       3623       3623          0      4.3   0.03
  proveedor-005       2965       2965          0      4.2   0.02
  proveedor-006       2502       2502          0      4.3   0.02
  proveedor-007       2160       2160          0      4.4   0.02
  proveedor-008       1898       1898          0      4.7   0.02
  proveedor-009       1690       1690          0      4.7   0.01
  (se listan los 10 de mayor s y cualquier otro saturado o con pendientes)

Colas:
  consultas.fifo       visibles=18714 en_vuelo=2
  consultas-dlq.fifo   visibles=0 en_vuelo=0

Mock de la API (desde su arranque):
  violaciones                       0
  requests                          85862
  máximo en vuelo, todos los prov.  22
  máximo en vuelo de un proveedor   1
  proveedores con máximo > 1        ninguno
```
