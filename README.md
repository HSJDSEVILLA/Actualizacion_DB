# SincronizaBD — Sincronización MySQL → Oracle

## Descripción
Este script sincroniza tablas seleccionadas desde una base de datos **MySQL** hacia **Oracle**, realizando una transferencia controlada de datos.  

## Características principales
- **Extracción por lotes** desde MySQL mediante consultas configurables.
- **Inserción en Oracle** usando `MERGE` con modo *solo-insert* (no duplica registros).
- **Conversión automática de tipos de datos** (`DATE`, `TIMESTAMP`, `INTERVAL`).
- **Validación numérica** para evitar errores `ORA-01438` (desbordes en `NUMBER(p,s)`).
- **Gestión de errores**:
  - Genera ficheros CSV y LOG solo si existen rechazos.
  - Los archivos se guardan en la carpeta `out/` junto al ejecutable.
- **Resumen de ejecución en terminal** muestra filas leídas, aceptadas y número de rechazos.
- **Diseño modular** (configurable por tabla y adaptable a otros esquemas).

## Tablas sincronizadas por defecto
- `booked_history  → BOOKED_HISTORY`
- `saafe_operations → SAAFE_OPERATIONS`
- `saafe_authorizations → SAAFE_AUTHORIZATIONS`
- `medical_acts_history → MEDICAL_ACTS_HISTORY`

Cada una se filtra por las filas del **último día completo** (`datum` dentro del rango de ayer).
