# Calidad de datos en el pipeline medallion

Fuente: consultas SQL sobre las tablas de Unity Catalog después de la corrida del 2026-11-19. Telemetría de 385 tanques del 2023-01-01 al 2026-11-18.

| Tabla | Filas |
| --- | --- |
| bronze.telemetria_inventario | 525,417 |
| silver.cuarentena_telemetria | 778 |
| silver.telemetria_depurada | 524,132 |
| silver.fact_inventario_tanque_diario | 524,910 |
| gold.serie_forecast_instalacion_producto | 334,245 |
| ml.pronostico_capacidad_diario | 7,260 (242 series x 30 días) |

## Conciliación

1. **Duplicados:** Bronze tiene 525,417 lecturas y 524,910 llaves únicas `(id_tanque, fecha_lectura)`: 507 duplicados que Auto CDC resuelve.
2. **Cuarentena:** 778 lecturas rechazadas por las expectativas, que se apartan en lugar de perderse:

| Motivo de rechazo | Filas |
| --- | --- |
| volumen_no_nulo, volumen_no_negativo, volumen_dentro_capacidad | 513 |
| volumen_dentro_capacidad (picos sobre la capacidad nominal) | 265 |

3. **Telemetría depurada:** 524,132 filas, 524,132 llaves únicas y 0 volúmenes nulos.
4. **Hecho diario sin huecos:** 524,910 filas = 524,132 lecturas válidas + 778 imputadas (`es_imputado`), es decir, la rejilla diaria completa.

El ruido se inyectó a propósito en el generador (~0.1% nulos, ~0.05% picos, ~0.1% duplicados) para ejercitar estas reglas.
