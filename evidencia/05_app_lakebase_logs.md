# App de chat: logs y registro en Lakebase

## Extractos de logs de la App (fuente APP)

Arranque con la identidad del service principal: crea o verifica el esquema y lee el plan de acción por el SQL warehouse.

```text
2026-11-19 02:15:44 INFO init_app INIT_OK esquema=pemex_chat owner="<APP_SP_CLIENT_ID>" current_user=<APP_SP_CLIENT_ID>
2026-11-19 02:15:44 INFO init_app PLAN_OK filas=9 corte=2026-11-17 decisiones_vigentes=0
```

Smoke test de arranque como service principal (`RUN_SMOKE_TEST=true`, 13-nov):

```text
2026-11-13 00:39:47 INFO init_app SMOKE_OK ruta=genie latencia_ms=17979 sql=True error=None
2026-11-13 00:39:49 INFO init_app SMOKE_OK ruta=modelo latencia_ms=1903 sql=False error=None
```

Uso real desde la UI:

```text
2026-11-18 21:47:02 INFO servicio respuesta ruta=genie latencia_ms=161893 error=None
2026-11-18 21:51:05 INFO servicio respuesta ruta=genie latencia_ms=110538 error=None
2026-11-18 22:46:17 INFO servicio respuesta ruta=modelo latencia_ms=78733 error=None
2026-11-19 03:16:20 INFO pemex-chat decision_id=1 serie=REF-CAD|COMB decision=APROBADA usuario=<USER_EMAIL>
2026-11-19 03:16:39 INFO pemex-chat decision_id=2 serie=TMR-LZC|COMB decision=APROBADA usuario=<USER_EMAIL>
2026-11-19 03:17:04 INFO pemex-chat decision_id=3 serie=TMR-PRO|COMB decision=APROBADA usuario=<USER_EMAIL>
```

Las latencias altas (78-162 s) corresponden a arranques en frío del SQL warehouse y del endpoint con scale-to-zero; con todo encendido, Genie responde en unos 18 s y el modelo en unos 2 s (smoke test).

## Lo que quedó en Lakebase (`pemex_chat`)

Consultas de solo lectura del 2026-11-19. El esquema y sus 3 tablas los creó el service principal de la App al arrancar.

* `conversaciones`: 4 conversaciones de 2 usuarios (el usuario de prueba `smoke-test` y el usuario de la demo).
* `mensajes` (respuestas del asistente):

| Ruta | Respuestas | Con error | Latencia mediana |
| --- | --- | --- | --- |
| genie | 4 | 0 | 65.3 s |
| modelo | 4 | 0 | 2.2 s |

* `decisiones` (pestaña Plan de acción):

| decision_id | Fecha de corte | Serie | Decisión | Alternativa / volumen | Comentario del usuario |
| --- | --- | --- | --- | --- | --- |
| 1 | 2026-11-17 | REF-CAD\|COMB | APROBADA | NINGUNA / 0 bbl | adelantaremos los depachos |
| 2 | 2026-11-17 | TMR-LZC\|COMB | APROBADA | TMR-ACA / 8,152 bbl | se va a diferrir |
| 3 | 2026-11-17 | TMR-PRO\|COMB | APROBADA | REF-OLM / 3,642 bbl | Se hizo la transferencia |

La alternativa y el volumen de cada decisión coinciden con la recomendación de `plan_accion_alertas` para la misma serie y corte.
