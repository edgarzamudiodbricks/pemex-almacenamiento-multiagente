# Arquitectura

```text
Generador (notebook) --> Volume landing (CSV/JSON)
        |
        v
Lakeflow SDP: Bronze --> Silver --> Gold ----------------------+
                                     |                          |
                                     v                          v
                          Modelo LightGBM (MLflow/UC)     Genie Agent / Dashboard
                                     |
                     pronostico_capacidad_diario, alertas_capacidad
                                     |
                          Model Serving (endpoint)
                                     |
App de chat (Streamlit) --router--> Genie (datos) | Endpoint (what-if)
        |
        v
Lakebase (historial de conversaciones)
```

## 1. Datos sintéticos

* 59 instalaciones (refinerías, terminales marítimas, terminales de crudo y TAD), 385 tanques y 9 productos.
* Telemetría diaria por tanque desde 2023-01-01: volumen, recibos, despachos, temperatura y fuente.
* Valores determinísticos por `(tanque, fecha)`: reprocesar una fecha produce el mismo resultado.
* Ruido intencional para probar las validaciones: ~0.1% volúmenes nulos, ~0.05% picos al 135% de capacidad, ~0.1% duplicados.
* Modo `incremental`: continúa desde la última fecha cargada en Bronze (catch-up automático).

## 2. Pipeline medallion (Lakeflow Spark Declarative Pipelines)

* **Fuente:** volume `landing` con Auto Loader (telemetría) y materialized views (catálogos).
* **Transformaciones:**
  * Bronze: ingesta cruda con evolución de esquema (`addNewColumns`).
  * Silver: dimensiones, mantenimientos, cuarentena de lecturas inválidas, `telemetria_depurada` con Auto CDC SCD1 (llave `id_tanque` + `fecha_lectura`) y `fact_inventario_tanque_diario` sin huecos (marca `es_imputado`).
  * Gold: inventario diario por instalación-producto, snapshots actuales, resumen mensual y `serie_forecast_instalacion_producto` (242 series).
* **Actualización:** triggered, serverless; el Job la ejecuta diario sin full refresh.
* **Errores y calidad:** expectativas en Silver; las lecturas inválidas van a cuarentena en lugar de perderse; duplicados resueltos por Auto CDC (idempotente ante reprocesos).

## 3. Modelo de pronóstico

* Objetivo: `pct_llenado` (0-100) por `serie_id = id_instalacion|id_producto`, horizonte 30 días.
* LightGBM global con lags >= 30 días, medias móviles, calendario y `frac_capacidad_operativa` (capacidad operativa / nominal) como covariable conocida a futuro; tres modelos: p50 y cuantiles p10/p90.
* Validación en el desarrollo inicial (holdout de 30 días): MASE 1.479 contra 2.971 del naive y 3.152 del seasonal naive (7 d); cobertura p10-p90 de 68.2%. Cada reentrenamiento registra sus métricas en el experimento MLflow.
* Registro en Unity Catalog con alias `champion`; firma pyfunc: entrada `serie_id`, `fecha`, `frac_capacidad_operativa`; salida `pct_llenado_p10/p50/p90`.
* El notebook `03_actualizar_endpoint_pronostico` mueve el endpoint a la versión `champion` y valida que coincida con la tabla de pronóstico.

## 3b. Plan de acción por alerta (IA generativa)

* Notebook `03_ml/04_plan_accion_alertas`, tarea 5 del Job (después de reentrenar y pronosticar).
* Por cada alerta vigente arma un contexto con la alerta, el estado actual, el pronóstico p10/p50/p90 en días clave y hasta 3 instalaciones de la misma región y producto con capacidad libre.
* `ai_query` (modelo de `system.ai`, salida estructurada, temperatura 0) redacta acción, justificación, alternativa, volumen, fecha límite y riesgo si no se actúa.
* Validaciones en código: la alternativa debe estar en el contexto, el volumen no puede exceder su capacidad libre y la prioridad se fija por regla (SATURACION = ALTA, RIESGO_SATURACION = MEDIA), no por el LLM.
* Destino `pemex_alm_ml.plan_accion_alertas`, idempotente por `fecha_corte` (`replaceWhere`); guarda el contexto exacto enviado al LLM para trazabilidad. Si todas las llamadas fallan, la tarea falla y no escribe.

## 4. App de chat

* `router.py`: reglas deterministas. Escenarios ("qué pasa si", "simula", cambios de capacidad) y pronósticos de una serie concreta van al modelo; todo lo demás a Genie (incluye rankings de alertas).
* `escenario.py`: resuelve instalación y producto (por clave, nombre o alias), interpreta el cambio de capacidad (absoluto "a 80%" o relativo "baja 20%"), lee la base del pronóstico y llama al endpoint para base y escenario por separado.
* `genie_client.py`: Genie Conversation API; mantiene el `conversation_id` por conversación para preguntas de seguimiento.
* `storage.py` + `init_app.py`: el service principal crea el esquema `pemex_chat` al arrancar (queda como dueño); pool de conexiones con token OAuth renovado por conexión.
* Las preguntas de seguimiento sin instalación reutilizan la última serie de la conversación.
* `plan_accion.py` + pestaña "Plan de acción": lee `plan_accion_alertas` por el SQL warehouse y permite APROBADA, MODIFICADA o RECHAZADA (rechazar exige comentario). Cada decisión se guarda en Lakebase; la última por serie es la vigente.

### Tablas en Lakebase (`pemex_chat`)

* `conversaciones`: `conversacion_id`, `usuario`, `titulo`, `genie_conversation_id`, `creado_en`, `actualizado_en`.
* `mensajes`: `mensaje_id`, `conversacion_id`, `rol`, `contenido`, `ruta` (genie/modelo), `genie_conversation_id`, `genie_message_id`, `sql_generado`, `parametros_escenario` (JSONB), `resultado` (JSONB), `latencia_ms`, `error`, `creado_en`.
* `decisiones`: `decision_id`, `fecha_corte`, `serie_id`, `decision`, `accion_recomendada`, `accion_final`, `instalacion_alternativa`, `volumen_sugerido_bbl`, `comentario`, `usuario`, `creado_en`.

## 5. Orquestación

Job diario (06:00 America/Mexico_City): generar datos -> refresh del pipeline -> reentrenar y pronosticar -> (actualizar endpoint | plan de acción por alerta). Cada tarea tiene 1 reintento y depende del éxito de la anterior.
