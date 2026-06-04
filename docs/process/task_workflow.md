# Flujo Local De Tareas

## Fuente De Verdad

La fuente de verdad de las tareas del plan de validacion es:

`docs/tasks/repiq_14d_tasks.json`

El tablero visual se genera desde ese archivo en:

`artifacts/tasks/repiq_14d_board.html`

El tablero HTML esta en `artifacts/`, por lo que no se versiona. El JSON si se versiona.

## Estados

- `backlog`: tarea definida, todavia no lista para trabajar.
- `ready`: siguiente tarea elegible.
- `in_progress`: tarea activa.
- `review`: implementada, pendiente de revision o validacion manual.
- `blocked`: bloqueada por decision, dato externo o problema tecnico.
- `done`: cerrada con evidencia minima.

## Comandos

Listar tareas activas:

```bash
python3 scripts/tasks.py list
```

Ver detalle:

```bash
python3 scripts/tasks.py show T01
```

Empezar una tarea:

```bash
python3 scripts/tasks.py start T01
```

Anotar contexto:

```bash
python3 scripts/tasks.py note T01 "Repositorio creado; falta test de indices."
```

Mandar a revision:

```bash
python3 scripts/tasks.py review T01 --tests "pytest tests/unit/test_benchmark_repositories.py"
```

Cerrar:

```bash
python3 scripts/tasks.py done T01 --tests "pytest tests/unit/test_benchmark_repositories.py"
```

Bloquear:

```bash
python3 scripts/tasks.py block T01 --note "Falta decidir si benchmark_businesses sera coleccion propia o extension de crm_leads."
```

Regenerar tablero:

```bash
python3 scripts/tasks.py board
```

## Regla De Implementacion

Antes de tocar codigo:

1. Ejecutar `python3 scripts/tasks.py list --status ready`.
2. Ejecutar `python3 scripts/tasks.py show <TASK_ID>`.
3. Marcar la tarea con `python3 scripts/tasks.py start <TASK_ID>`.

Durante el trabajo:

1. Mantener el alcance del cambio dentro del ticket activo.
2. Si aparece trabajo nuevo, crear o anotar tarea; no mezclarlo sin necesidad.
3. Registrar decisiones importantes con `python scripts/tasks.py note`.

Al terminar:

1. Ejecutar tests relevantes.
2. Registrar los tests con `--tests`.
3. Mover a `done` si queda validado o a `review` si necesita mirada manual.
4. Abrir `artifacts/tasks/repiq_14d_board.html` para revisar el estado visual si hace falta.

## Uso Con Codex

Cuando se pida implementar una parte del plan, se debe trabajar desde el ticket correspondiente.

Ejemplo:

```bash
python3 scripts/tasks.py start T03
# implementar selector de competidores
python3 scripts/tasks.py done T03 --tests "pytest tests/unit/test_competitor_selector.py"
```

Si el usuario pide algo que no esta en el plan, se crea una tarea nueva antes de implementar:

```bash
python3 scripts/tasks.py add T11 "Nueva tarea" --area backend --type technical --priority P1 --status ready
```

## Tablero Tipo Trello En Local

El flujo minimo no depende de Trello, Kanboard ni servicios externos.

Para verlo como tablero:

```bash
python3 scripts/tasks.py board --open
```

Esto abre el HTML local generado desde el JSON. Mas adelante se puede anadir un adaptador de sincronizacion a Kanboard o Trello sin cambiar la fuente de verdad.
