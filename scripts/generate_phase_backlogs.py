from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKLOG_DIR = ROOT / "docs" / "backlogs"


PHASES = [
    {
        "id": "fase_00",
        "file": "fase_00_baseline_actual_google_maps_backlog.md",
        "title": "Baseline actual Google Maps",
        "kind": "baseline",
        "phase_doc": "docs/context/phases/fase_00_baseline_actual_google_maps.md",
        "focus": "estabilizar y congelar el baseline actual (Google Maps + jobs + analisis + eventos)",
        "keywords": "cookies, sesion, scrolling, cache, serializacion, smoke tests",
        "done": [
            "flujo E2E reproducible en local/dev",
            "errores comunes con diagnostico util",
            "metricas minimas de baseline disponibles",
            "documentacion operativa actualizada",
        ],
        "notes": [
            "No hacer refactor grande: foco en estabilidad y observabilidad.",
            "Estandarizar scripts y smoke tests antes de Fase 1.",
            "Documentar limites reales y mitigaciones actuales.",
        ],
    },
    {
        "id": "fase_01",
        "file": "fase_01_arquitectura_modular_y_workers_backlog.md",
        "title": "Arquitectura modular y workers",
        "kind": "architecture",
        "phase_doc": "docs/context/phases/fase_01_arquitectura_modular_y_workers.md",
        "focus": "separar scraping, analisis y reporte en workers con contratos de mensajes listos para RabbitMQ",
        "keywords": "scrape-worker, analysis-worker, report-worker, orchestrator, payloads, broker abstraction",
        "done": [
            "contratos de mensajes y estados de job definidos",
            "servicios/casos de uso separados con API compatible",
            "workers especializados ejecutables",
            "plan de migracion a RabbitMQ definido",
        ],
        "notes": [
            "Definir contratos antes de mover codigo.",
            "Mantener endpoints actuales como fachada estable.",
            "Migracion a RabbitMQ incremental, no de golpe.",
        ],
    },
    {
        "id": "fase_02",
        "file": "fase_02_modelo_canonico_negocio_y_fuentes_backlog.md",
        "title": "Modelo canonico de negocio y fuentes",
        "kind": "canonical",
        "phase_doc": "docs/context/phases/fase_02_modelo_canonico_negocio_y_fuentes.md",
        "focus": "centralizar negocio, perfiles de fuente y menciones/reviews en un modelo canonico multi-fuente",
        "keywords": "business, source_profile, mention, review, matching, raw_payload, ciudad, pais, idioma",
        "done": [
            "Business y SourceProfile definidos y persistidos",
            "Mention/Review canonicos con dedupe basico",
            "matching reusable por conectores",
            "consultas/API desacopladas del modelo legacy",
        ],
        "notes": [
            "Modelar primero, migrar Google Maps despues.",
            "Guardar siempre raw_payload.",
            "Mantener compatibilidad de endpoints con DTOs/adaptadores.",
        ],
    },
    {
        "id": "fase_03",
        "file": "fase_03_scraper_tripadvisor_backlog.md",
        "title": "Scraper Tripadvisor",
        "kind": "source",
        "phase_doc": "docs/context/phases/fase_03_scraper_tripadvisor.md",
        "focus": "implementar conector vertical de Tripadvisor integrado en la pipeline y normalizado al modelo canonico",
        "keywords": "tripadvisor, discovery, parser estructural, matching, smoke tests",
        "source_name": "Tripadvisor",
        "source_key": "tripadvisor",
        "strong_signal": "ciudad/pais + ficha/URL del negocio",
        "done": [
            "discovery y matching minimo funcional",
            "extraccion y normalizacion al modelo canonico",
            "integracion con jobs y eventos",
            "smoke tests y limites operativos documentados",
        ],
        "notes": [
            "Primero discovery/matching, luego volumen.",
            "Usar patrones estructurales y fallbacks; evitar IDs fragiles.",
            "Separar browser_flow, parser y normalizador.",
        ],
    },
    {
        "id": "fase_04",
        "file": "fase_04_scraper_trustpilot_backlog.md",
        "title": "Scraper Trustpilot",
        "kind": "source",
        "phase_doc": "docs/context/phases/fase_04_scraper_trustpilot.md",
        "focus": "implementar conector vertical de Trustpilot integrado en la pipeline y normalizado al modelo canonico",
        "keywords": "trustpilot, dominio web, parser, normalizador, pipeline",
        "source_name": "Trustpilot",
        "source_key": "trustpilot",
        "strong_signal": "dominio web oficial del negocio",
        "done": [
            "discovery y matching minimo funcional",
            "extraccion y normalizacion al modelo canonico",
            "integracion con jobs y eventos",
            "smoke tests y limites operativos documentados",
        ],
        "notes": [
            "Priorizar coincidencia por dominio web cuando exista.",
            "Separar discovery/matching del parser.",
            "Controlar limites y bloqueos desde configuracion.",
        ],
    },
    {
        "id": "fase_05",
        "file": "fase_05_scraper_reddit_backlog.md",
        "title": "Scraper Reddit",
        "kind": "source",
        "phase_doc": "docs/context/phases/fase_05_scraper_reddit.md",
        "focus": "implementar conector de Reddit (preferiblemente API oficial) para menciones no estructuradas",
        "keywords": "reddit API, mentions, relevancia, subreddit, engagement",
        "source_name": "Reddit",
        "source_key": "reddit",
        "strong_signal": "nombre + ciudad + contexto del hilo/subreddit",
        "done": [
            "recoleccion con API oficial o adaptador definido",
            "menciones normalizadas al modelo canonico",
            "integracion con pipeline y observabilidad",
            "reglas de relevancia y limites de uso documentados",
        ],
        "notes": [
            "Priorizar API oficial y limites de uso.",
            "Tratar Reddit como source de mentions, no solo reviews.",
            "Separar relevancia de entity matching.",
        ],
    },
    {
        "id": "fase_06",
        "file": "fase_06_refinamiento_analisis_llm_rag_backlog.md",
        "title": "Refinamiento de analisis LLM y RAG",
        "kind": "llm",
        "phase_doc": "docs/context/phases/fase_06_refinamiento_analisis_llm_rag.md",
        "focus": "mejorar calidad y control del analisis con modos, prompts versionados, batchers, reanalyze y RAG",
        "keywords": "analysis_mode, prompt versioning, reanalyze, batching, RAG, coste, latencia",
        "done": [
            "modos de analisis definidos y seleccionables",
            "prompts versionados y metadata persistida",
            "reanalyze con batchers configurable",
            "metricas de coste/latencia y fallbacks",
        ],
        "notes": [
            "Definir modos y contratos de salida primero.",
            "Versionar prompts y medir calidad con dataset.",
            "Usar reanalyze sobre datos guardados para iterar rapido.",
        ],
    },
    {
        "id": "fase_07",
        "file": "fase_07_informe_estructurado_backlog.md",
        "title": "Informe estructurado",
        "kind": "report",
        "phase_doc": "docs/context/phases/fase_07_informe_estructurado.md",
        "focus": "transformar analisis en informe estructurado con renderer y report-worker",
        "keywords": "ReportData, renderer, PDF, Typst, HTML->PDF, report-worker, descarga",
        "done": [
            "esquema de informe versionado",
            "renderer v1 genera artefacto real",
            "report-worker integrado en pipeline",
            "criterios de calidad del informe v1 definidos",
        ],
        "notes": [
            "DiseÃ±ar `ReportData` antes del renderer.",
            "Separar contenido del layout visual.",
            "Integrar report-worker con estados/eventos.",
        ],
    },
    {
        "id": "fase_08",
        "file": "fase_08_interfaz_mvp_analisis_backlog.md",
        "title": "Interfaz MVP de analisis",
        "kind": "ui",
        "phase_doc": "docs/context/phases/fase_08_interfaz_mvp_analisis.md",
        "focus": "crear una UI MVP para lanzar analisis, seguir progreso y consultar resultados/reportes",
        "keywords": "formulario, SSE, progreso, listados paginados, result view, demo",
        "done": [
            "formulario de analisis funcional",
            "progreso en tiempo real por SSE",
            "vista de resultados/listados/reportes usable",
            "smoke test UI y checklist de demo",
        ],
        "notes": [
            "Primero flujo principal: formulario -> job -> progreso -> resultado.",
            "Reutilizar SSE y endpoints existentes.",
            "Cerrar demo interna antes de refinamientos visuales mayores.",
        ],
    },
    {
        "id": "fase_09",
        "file": "fase_09_landing_demo_y_email_backlog.md",
        "title": "Landing, demo y email",
        "kind": "landing",
        "phase_doc": "docs/context/phases/fase_09_landing_demo_y_email.md",
        "focus": "captar leads con landing y enviar mini-analisis por email para agendar demo",
        "keywords": "landing, lead capture, mini-analisis, email, funnel, compliance",
        "done": [
            "landing y formulario funcionales",
            "captura de leads + job mini-analisis + email operativo",
            "metricas basicas del funnel disponibles",
            "validaciones/anti-abuso y checklist compliance minima",
        ],
        "notes": [
            "Separar captacion, procesamiento y envio.",
            "Definir el producto mini-analisis antes de implementarlo.",
            "AÃ±adir trazabilidad de lead/job/email desde el inicio.",
        ],
    },
]


def num_from_phase(phase_id: str) -> str:
    return phase_id.split("_")[1]


def mk_ticket(phase_num: str, idx: int, title: str, kind: str, priority: str, desc: str, deps=None, accept=None, validate=None):
    return {
        "id": f"T{phase_num}-{idx:03d}",
        "title": title,
        "kind": kind,
        "priority": priority,
        "desc": desc.strip(),
        "deps": list(deps or []),
        "accept": list(accept or []),
        "validate": list(validate or []),
    }


def mk_epic(phase_num: str, idx: int, title: str, result: str, objective: str, tickets, deps=None, risks=None):
    return {
        "id": f"E{phase_num}-{idx:02d}",
        "title": title,
        "result": result.strip(),
        "objective": objective.strip(),
        "tickets": tickets,
        "deps": list(deps or []),
        "risks": list(risks or []),
    }


def build_common_epics(phase):
    n = num_from_phase(phase["id"])
    return [
        mk_epic(
            n,
            1,
            "DiseÃ±o funcional y contratos de la fase",
            f"La fase '{phase['title']}' tiene alcance, contratos y validaciones definidos.",
            f"Bajar a contratos y decisiones tecnicas el objetivo de la fase: {phase['focus']}.",
            [
                mk_ticket(
                    n,
                    1,
                    "Definir contrato de entrada/salida de la fase",
                    "Diseno",
                    "Alta",
                    f"Especificar que datos entran, que artefactos salen y como se valida el resultado de la fase. Contexto: {phase['keywords']}.",
                    accept=[
                        "Entradas y salidas definidas con ejemplos.",
                        "Precondiciones y dependencias explicitadas.",
                        "Criterio de validacion funcional acordado.",
                    ],
                    validate=["Revision tecnica del contrato contra el roadmap y el codigo actual."],
                ),
                mk_ticket(
                    n,
                    2,
                    "DiseÃ±ar plan de pruebas/smoke de la fase",
                    "QA",
                    "Alta",
                    "Definir pruebas minimas para validar la fase sin esperar al final: smoke, fixtures, pruebas de integracion y chequeos manuales.",
                    deps=[f"T{n}-001"],
                    accept=[
                        "Existe checklist de pruebas de la fase.",
                        "Cada prueba tiene comando/precondiciones.",
                        "Se definen criterios de exito/fallo.",
                    ],
                    validate=["Ejecucion de al menos una prueba de referencia o simulacion del flujo."],
                ),
                mk_ticket(
                    n,
                    3,
                    "Documentar supuestos, limites y riesgos",
                    "Documentacion",
                    "Media",
                    f"Registrar los limites esperados de la fase y riesgos principales para {phase['title']}, incluyendo deuda tecnica aceptada temporalmente.",
                    deps=[f"T{n}-001"],
                    accept=[
                        "Lista de riesgos y mitigaciones iniciales.",
                        "Supuestos tecnicos explicitados.",
                        "Deuda tecnica temporal identificada.",
                    ],
                    validate=["Revision documental por consistencia con fase/contexto."],
                ),
            ],
            risks=["SobrediseÃ±ar antes de validar el flujo real.", "No definir validacion temprana y llegar al final con incertidumbre."],
        ),
        mk_epic(
            n,
            2,
            "Implementacion del nucleo de la fase",
            f"Existe una implementacion funcional del bloque principal de la fase '{phase['title']}'.",
            f"Construir el nucleo tecnico que materializa la fase: {phase['focus']}.",
            [
                mk_ticket(
                    n,
                    4,
                    "Implementar componentes principales de la fase",
                    "Implementacion",
                    "Alta",
                    f"Desarrollar la pieza central de la fase (modulos, servicios, conectores o UI) manteniendo separacion de responsabilidades. Pistas: {phase['keywords']}.",
                    deps=[f"T{n}-001"],
                    accept=[
                        "La funcionalidad principal existe y se ejecuta.",
                        "Se respeta separacion de capas/modulos razonable.",
                        "El codigo sigue contratos definidos en la fase.",
                    ],
                    validate=["Smoke test del flujo principal de la fase."],
                ),
                mk_ticket(
                    n,
                    5,
                    "Configurar parametros, limites y flags de ejecucion",
                    "Implementacion",
                    "Media",
                    "Exponer configuracion minima de timeouts, limites, modos o estrategias para poder operar y depurar sin tocar codigo continuamente.",
                    deps=[f"T{n}-004"],
                    accept=[
                        "Parametros principales centralizados.",
                        "Defaults razonables definidos.",
                        "Se pueden sobreescribir para pruebas.",
                    ],
                    validate=["Probar ejecucion con defaults y con una variacion de parametros."],
                ),
                mk_ticket(
                    n,
                    6,
                    "Integrar la fase con la pipeline/API existente",
                    "Integracion",
                    "Alta",
                    "Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.",
                    deps=[f"T{n}-004"],
                    accept=[
                        "La integracion no rompe el baseline.",
                        "Estados/eventos/respuestas reflejan la nueva fase.",
                        "Se documenta el flujo de integracion.",
                    ],
                    validate=["E2E o integracion controlada con el sistema actual."],
                ),
            ],
            deps=[f"E{n}-01"],
            risks=["Acoplamiento excesivo con implementacion actual.", "Cambios internos rompiendo endpoints/contratos existentes."],
        ),
        mk_epic(
            n,
            3,
            "Observabilidad, calidad y robustez",
            f"La fase puede operarse con visibilidad y diagnostico suficiente.",
            "AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.",
            [
                mk_ticket(
                    n,
                    7,
                    "Instrumentar logs/eventos por etapas",
                    "Observabilidad",
                    "Media",
                    "Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.",
                    deps=[f"T{n}-006"],
                    accept=[
                        "Etapas clave emiten informacion de progreso.",
                        "Errores incluyen contexto suficiente.",
                        "Logs/eventos son consistentes con el resto del sistema.",
                    ],
                    validate=["Revisar logs/eventos en una ejecucion real de la fase."]),
                mk_ticket(
                    n,
                    8,
                    "AÃ±adir metricas minimas de rendimiento y resultado",
                    "Observabilidad",
                    "Media",
                    "Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.",
                    deps=[f"T{n}-007"],
                    accept=[
                        "Metricas minimas definidas y emitidas.",
                        "Se pueden consultar en logs/DB/eventos.",
                        "Sirven para detectar regresiones.",
                    ],
                    validate=["Ejecutar una corrida y revisar metricas registradas."]),
                mk_ticket(
                    n,
                    9,
                    "Endurecer manejo de errores y retries/fallbacks",
                    "Bugfix/Implementacion",
                    "Media",
                    "Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.",
                    deps=[f"T{n}-007"],
                    accept=[
                        "Errores comunes generan mensajes accionables.",
                        "Se aplican retries/fallbacks donde tenga sentido.",
                        "Fallos no dejan recursos abiertos o estados inconsistentes.",
                    ],
                    validate=["Provocar 1-2 fallos controlados y revisar comportamiento."]),
            ],
            deps=[f"E{n}-02"],
            risks=["Falta de visibilidad real del comportamiento en runtime.", "Errores silenciosos o mensajes poco accionables."],
        ),
        mk_epic(
            n,
            4,
            "Salida de fase y deuda tecnica controlada",
            f"La fase '{phase['title']}' cierra con criterio objetivo y backlog de siguientes mejoras.",
            "Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.",
            [
                mk_ticket(
                    n,
                    10,
                    "Ejecutar validacion final E2E / de fase",
                    "QA",
                    "Alta",
                    "Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.",
                    deps=[f"T{n}-002", f"T{n}-006", f"T{n}-008"],
                    accept=[
                        "Se ejecuta validacion final definida.",
                        "Resultados y evidencias quedan registrados.",
                        "Problemas se convierten en tickets o deuda.",
                    ],
                    validate=["Checklist de salida completada."]),
                mk_ticket(
                    n,
                    11,
                    "Documentar estado de salida y decisiones de fase",
                    "Documentacion",
                    "Media",
                    "Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.",
                    deps=[f"T{n}-010"],
                    accept=[
                        "Docs de fase y/o arquitectura actualizados.",
                        "Se listan limites y decisiones de diseÃ±o.",
                        "Se enlazan artefactos y pruebas relevantes.",
                    ],
                    validate=["Revision documental final."]),
                mk_ticket(
                    n,
                    12,
                    "Priorizar backlog de continuidad (siguiente fase/iteracion)",
                    "Planificacion",
                    "Media",
                    "Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.",
                    deps=[f"T{n}-010", f"T{n}-011"],
                    accept=[
                        "Pendientes priorizados y categorizados.",
                        "Se identifican riesgos si se difieren.",
                        "La siguiente fase recibe inputs claros.",
                    ],
                    validate=["Revision del backlog de continuidad contra roadmap."]),
            ],
            deps=[f"E{n}-03"],
            risks=["Cerrar la fase sin criterios objetivos.", "No capturar deuda tecnica y repetir errores en la siguiente fase."],
        ),
    ]


def specialize(phase, epics):
    kind = phase["kind"]
    n = num_from_phase(phase["id"])
    if kind == "source":
        s = phase["source_name"]
        key = phase["source_key"]
        signal = phase["strong_signal"]
        epics[0]["title"] = f"Discovery y matching de {s}"
        epics[0]["result"] = f"El sistema localiza la entidad correcta en {s} y la valida con matching explicable."
        epics[0]["objective"] = f"Definir y validar discovery + entity matching en {s}, usando como seÃ±al fuerte {signal}."
        epics[0]["tickets"][0]["title"] = f"DiseÃ±ar flujo de discovery en {s}"
        epics[0]["tickets"][0]["desc"] = f"Documentar como localizar la ficha/contenido objetivo en {s}: busqueda, filtros, seleccion y seÃ±ales de identidad. SeÃ±al fuerte: {signal}."
        epics[0]["tickets"][1]["title"] = f"Implementar cliente/flujo de acceso para {s}"
        epics[0]["tickets"][1]["desc"] = f"Crear `browser_flow.py` o `api_client.py` para {s}, separado del parser, que devuelva payload bruto y metadatos de discovery."
        epics[0]["tickets"][2]["title"] = f"Implementar matching minimo para `{key}`"
        epics[0]["tickets"][2]["desc"] = f"Aplicar el servicio de matching sobre candidatos de {s} y registrar `match_score`, decision y motivo resumido."

        epics[1]["title"] = f"Parser y normalizador de {s}"
        epics[1]["result"] = f"El conector `{key}` extrae datos de {s} y los normaliza al modelo canonico."
        epics[1]["objective"] = f"Separar parser estructural y normalizador para `{key}` evitando selectores fragiles."
        epics[1]["tickets"][0]["title"] = f"Implementar parser estructural para {s}"
        epics[1]["tickets"][0]["desc"] = f"Construir parser basado en patrones estructurales/semanticos en {s}, con fallbacks, evitando IDs dinamicos siempre que sea posible."
        epics[1]["tickets"][1]["title"] = f"Implementar normalizador `{key}` -> Mention/Review"
        epics[1]["tickets"][1]["desc"] = f"Mapear campos de {s} al modelo canonico (`source`, `source_item_id`, `url`, `text`, `rating/engagement`, `raw_payload`)."
        epics[1]["tickets"][2]["title"] = f"Persistir corrida de `{key}` con trazabilidad"

        epics[2]["title"] = f"Robustez operativa y observabilidad de {s}"
        epics[2]["tickets"][0]["title"] = f"Configurar limites/retries/volumen para `{key}`"
        epics[2]["tickets"][1]["title"] = f"Emitir eventos por etapas del conector `{key}`"
        epics[2]["tickets"][2]["title"] = f"Documentar limites y riesgos de uso de {s}"

        epics[3]["title"] = f"Integracion E2E de `{key}` en la pipeline"
        epics[3]["result"] = f"{s} participa en jobs de scraping/analisis sin romper el baseline existente."
        epics[3]["tickets"][0]["title"] = f"Registrar source `{key}` en scrape-worker/orquestador"
        epics[3]["tickets"][1]["title"] = f"Crear smoke tests y fixtures de `{key}`"
        epics[3]["tickets"][2]["title"] = f"Definir criterio de salida operativa de `{key}`"

    if kind == "architecture":
        epics[0]["title"] = "Contratos de mensajes y estados de job por etapa"
        epics[0]["objective"] = "Definir payloads y estados de `scrape-worker`, `analysis-worker` y `report-worker`, base de la orquestacion y RabbitMQ."
        epics[1]["title"] = "Refactor de orquestacion y puertos/adaptadores"
        epics[1]["tickets"][0]["title"] = "Separar query services, orchestration y analyze use cases"
        epics[1]["tickets"][1]["title"] = "Introducir puertos para scraper y proveedor LLM"
        epics[2]["title"] = "Workers especializados y colas logicas"
        epics[2]["tickets"][0]["title"] = "Crear entrypoints `scrape-worker`, `analysis-worker`, `report-worker`"
        epics[2]["tickets"][1]["title"] = "Introducir `queue_name`/`routing_key` internos"
        epics[2]["tickets"][2]["title"] = "Configurar concurrencia y timeouts por worker"
        epics[3]["title"] = "Preparacion e integracion gradual con RabbitMQ"
        epics[3]["tickets"][0]["title"] = "DiseÃ±ar topologia RabbitMQ (exchanges/queues/DLQ)"
        epics[3]["tickets"][1]["title"] = "Crear abstraccion de broker y adaptadores"
        epics[3]["tickets"][2]["title"] = "Plan de migracion incremental con rollback"

    if kind == "canonical":
        epics[0]["title"] = "Modelo `Business` y `SourceProfile`"
        epics[0]["tickets"][0]["title"] = "DiseÃ±ar entidad `Business` enriquecida"
        epics[0]["tickets"][1]["title"] = "DiseÃ±ar entidad `SourceProfile` por negocio/fuente"
        epics[0]["tickets"][2]["title"] = "Implementar repositorios e indices de negocio/fuentes"
        epics[1]["title"] = "Modelo canonico de `Mention`/`Review`"
        epics[1]["tickets"][0]["title"] = "DiseÃ±ar `Mention` y `ReviewMention`"
        epics[1]["tickets"][1]["title"] = "Implementar repositorio canonico con dedupe basico"
        epics[1]["tickets"][2]["title"] = "Migrar Google Maps al modelo canonico"
        epics[2]["title"] = "Entity matching reusable y explicable"
        epics[2]["tickets"][0]["title"] = "Definir scoring y umbrales de matching"
        epics[2]["tickets"][1]["title"] = "Implementar servicio de matching central"
        epics[2]["tickets"][2]["title"] = "DiseÃ±ar flujo de candidatos ambiguos/manual review"
        epics[3]["title"] = "Consultas/API sobre el modelo canonico"
        epics[3]["tickets"][0]["title"] = "Crear query services para negocio/fuentes/menciones"
        epics[3]["tickets"][1]["title"] = "AÃ±adir endpoints de SourceProfile/estado de sync"
        epics[3]["tickets"][2]["title"] = "Plan de deprecacion de estructuras legacy"

    if kind == "llm":
        epics[0]["title"] = "Modos de analisis y contratos de salida"
        epics[0]["tickets"][0]["title"] = "DiseÃ±ar catalogo de modos (`demo`, `deep`, `sentiment`, `solutions`)"
        epics[0]["tickets"][1]["title"] = "Definir DTOs/esquemas de salida por modo"
        epics[0]["tickets"][2]["title"] = "Exponer `analysis_mode` en analyze/reanalyze/jobs"
        epics[1]["title"] = "Prompts versionados y evaluacion"
        epics[1]["tickets"][0]["title"] = "Externalizar prompts con versionado"
        epics[1]["tickets"][1]["title"] = "Crear dataset de evaluacion y rubric"
        epics[1]["tickets"][2]["title"] = "Persistir metadata de analisis (prompt/modelo/batcher)"
        epics[2]["title"] = "Batching, reanalyze avanzado y RAG minimo"
        epics[2]["tickets"][0]["title"] = "DiseÃ±ar estrategias de batching por volumen/modo"
        epics[2]["tickets"][1]["title"] = "Implementar reanalyze configurable y comparativas"
        epics[2]["tickets"][2]["title"] = "Implementar RAG minimo para modos de profundidad"
        epics[3]["title"] = "Coste, latencia y fallbacks operativos"
        epics[3]["tickets"][0]["title"] = "Registrar metricas de coste/tokens/latencia"
        epics[3]["tickets"][1]["title"] = "Implementar fallbacks por cuota/coste/timeout"
        epics[3]["tickets"][2]["title"] = "Definir calidad minima por modo para produccion"

    if kind == "report":
        epics[0]["title"] = "Modelo de datos del informe (`ReportData`)"
        epics[0]["tickets"][0]["title"] = "DiseÃ±ar esquema `ReportData` y secciones"
        epics[0]["tickets"][1]["title"] = "Implementar mapper analisis -> `ReportData`"
        epics[0]["tickets"][2]["title"] = "Versionar plantillas de contenido por tipo de informe"
        epics[1]["title"] = "Renderer y formato de salida"
        epics[1]["tickets"][0]["title"] = "Comparar Typst vs HTML->PDF y elegir renderer"
        epics[1]["tickets"][1]["title"] = "Implementar renderer v1 y persistencia del artefacto"
        epics[1]["tickets"][2]["title"] = "DiseÃ±ar layout visual v1"
        epics[2]["title"] = "Integracion del report-worker y endpoints de informe"
        epics[2]["tickets"][0]["title"] = "Integrar `report-worker` con payloads/estados/eventos"
        epics[2]["tickets"][1]["title"] = "AÃ±adir metricas y logs de render"
        epics[2]["tickets"][2]["title"] = "Exponer endpoints de listado/descarga de informes"
        epics[3]["title"] = "Calidad de contenido y utilidad de negocio"
        epics[3]["tickets"][0]["title"] = "Estructurar recomendaciones accionables"
        epics[3]["tickets"][1]["title"] = "AÃ±adir metodologia y limitaciones"
        epics[3]["tickets"][2]["title"] = "Checklist de aceptacion del informe v1"

    if kind == "ui":
        epics[0]["title"] = "Formulario MVP y lanzamiento de analisis"
        epics[0]["tickets"][0]["title"] = "DiseÃ±ar formulario de analisis (inputs basicos y opcionales)"
        epics[0]["tickets"][1]["title"] = "Implementar envio a API y manejo de errores"
        epics[0]["tickets"][2]["title"] = "Persistir historial local de trabajos recientes"
        epics[1]["title"] = "Seguimiento en tiempo real por SSE"
        epics[1]["tickets"][0]["title"] = "DiseÃ±ar timeline/estado de job"
        epics[1]["tickets"][1]["title"] = "Implementar consumo SSE y reconexion basica"
        epics[1]["tickets"][2]["title"] = "Traducir errores tecnicos a mensajes accionables"
        epics[2]["title"] = "Visualizacion de resultados, listados y reportes"
        epics[2]["tickets"][0]["title"] = "DiseÃ±ar vista de resultado de analisis"
        epics[2]["tickets"][1]["title"] = "Consumir endpoints paginados de business/reviews/analyses"
        epics[2]["tickets"][2]["title"] = "Integrar acceso/preview de informes"
        epics[3]["title"] = "Preparacion de demo y calidad minima del MVP"
        epics[3]["tickets"][0]["title"] = "Documentar arranque frontend + API local/dev"
        epics[3]["tickets"][1]["title"] = "AÃ±adir smoke test UI del flujo principal"
        epics[3]["tickets"][2]["title"] = "Crear checklist de demo interna del MVP"

    if kind == "landing":
        epics[0]["title"] = "Producto de captacion y mini-analisis"
        epics[0]["tickets"][0]["title"] = "DiseÃ±ar estructura/copy de la landing"
        epics[0]["tickets"][1]["title"] = "Definir inputs minimos del formulario de lead"
        epics[0]["tickets"][2]["title"] = "Definir formato del mini-analisis por email"
        epics[1]["title"] = "Landing + captura de leads + persistencia"
        epics[1]["tickets"][0]["title"] = "Implementar landing y formulario MVP"
        epics[1]["tickets"][1]["title"] = "Crear endpoint de captura de lead y solicitud"
        epics[1]["tickets"][2]["title"] = "Modelar persistencia de leads y estados"
        epics[2]["title"] = "Mini-analisis asincrono y envio de email"
        epics[2]["tickets"][0]["title"] = "Definir flujo de mini-analisis (job) y limites"
        epics[2]["tickets"][1]["title"] = "Implementar plantilla de email y servicio de envio"
        epics[2]["tickets"][2]["title"] = "Integrar workers/pipeline en flujo de mini-analisis"
        epics[3]["title"] = "Operacion del funnel, anti-abuso y compliance"
        epics[3]["tickets"][0]["title"] = "Registrar metricas del funnel y seguimiento"
        epics[3]["tickets"][1]["title"] = "AÃ±adir validaciones y limites anti-abuso"
        epics[3]["tickets"][2]["title"] = "Checklist legal/compliance minima"

    return epics


def render_ticket(t):
    lines = [
        f"#### {t['id']} - {t['title']}",
        "",
        f"- `tipo`: `{t['kind']}`",
        f"- `prioridad`: `{t['priority']}`",
        "- `estado sugerido`: `TODO`",
        "",
        "**Descripcion**",
        "",
        t["desc"],
        "",
    ]
    if t["deps"]:
        lines += ["**Dependencias**", ""]
        lines += [f"- `{d}`" for d in t["deps"]]
        lines += [""]
    if t["accept"]:
        lines += ["**Criterios de aceptacion**", ""]
        lines += [f"{i}. {a}" for i, a in enumerate(t["accept"], start=1)]
        lines += [""]
    if t["validate"]:
        lines += ["**Validacion / prueba**", ""]
        lines += [f"- {v}" for v in t["validate"]]
        lines += [""]
    return "\n".join(lines).rstrip()


def render_epic(e):
    lines = [
        f"### {e['id']} - {e['title']}",
        "",
        "- `estado sugerido`: `TODO`",
        "",
        "**Resultado esperado**",
        "",
        e["result"],
        "",
        "**Objetivo de la epica**",
        "",
        e["objective"],
        "",
    ]
    if e["deps"]:
        lines += ["**Dependencias**", ""]
        lines += [f"- `{d}`" for d in e["deps"]]
        lines += [""]
    if e["risks"]:
        lines += ["**Riesgos a vigilar**", ""]
        lines += [f"- {r}" for r in e["risks"]]
        lines += [""]
    lines += ["**Tickets**", ""]
    for t in e["tickets"]:
        lines += [render_ticket(t), ""]
    return "\n".join(lines).rstrip()


def build_phase_doc(phase, epics):
    total_tickets = sum(len(e["tickets"]) for e in epics)
    summary_rows = "\n".join(
        f"| `{e['id']}` | {e['title']} | {len(e['tickets'])} | {e['result']} |" for e in epics
    )
    lines = [
        f"# Backlog {phase['id'].upper()} - {phase['title']}",
        "",
        f"- `fase`: `{phase['id']}`",
        f"- `documento de fase`: `{phase['phase_doc']}`",
        f"- `archivo backlog`: `docs/backlogs/{phase['file']}`",
        f"- `epicas`: `{len(epics)}`",
        f"- `tickets`: `{total_tickets}`",
        "- `estado general sugerido`: `TODO`",
        "",
        "## Objetivo operativo",
        "",
        phase["focus"],
        "",
        "## Contexto y foco de trabajo",
        "",
        f"Esta fase ataca el siguiente objetivo del roadmap: **{phase['focus']}**.",
        "",
        f"Palabras clave de implementacion/seguimiento: `{phase['keywords']}`.",
        "",
        "## Secuencia recomendada",
        "",
    ]
    lines += [f"{i}. {note}" for i, note in enumerate(phase["notes"], start=1)]
    lines += ["", "## Criterios de done de la fase", ""]
    lines += [f"{i}. {d}" for i, d in enumerate(phase["done"], start=1)]
    lines += [
        "",
        "## Resumen de epicas",
        "",
        "| ID | Epica | Tickets | Resultado esperado |",
        "| --- | --- | ---: | --- |",
        summary_rows,
        "",
        "## Epicas y tickets",
        "",
    ]
    for e in epics:
        lines += [render_epic(e), ""]
    return "\n".join(lines).rstrip() + "\n"


def build_backlog_for_phase(phase):
    epics = build_common_epics(phase)
    return specialize(phase, epics)


def build_readme(rows):
    return (
        "# Backlogs por fase\n\n"
        "Directorio de backlogs operativos del roadmap. Cada archivo aterriza la fase en epicas y tickets ejecutables.\n\n"
        "## Convenciones\n\n"
        "- IDs de epicas: `E##-XX`\n"
        "- IDs de tickets: `T##-XXX`\n"
        "- Estado inicial sugerido: `TODO`\n"
        "- Mantener IDs cuando migres tickets a GitHub Issues/Jira/Linear\n"
        "- Leer primero la fase en `docs/context/phases/` y despues este backlog\n\n"
        "## Indice\n\n"
        "| Fase | Archivo | Epicas | Tickets | Documento de fase |\n"
        "| --- | --- | ---: | ---: | --- |\n"
        f"{rows}\n\n"
        "## Uso recomendado\n\n"
        "1. Selecciona la fase activa y revisa dependencias.\n"
        "2. Pasa epicas/tickets al gestor que uses manteniendo IDs.\n"
        "3. Actualiza estados reales (`TODO`, `IN_PROGRESS`, `BLOCKED`, `DONE`).\n"
        "4. Al cerrar una fase, actualiza `docs/context/project_objective.md` y este backlog.\n"
    )


def main():
    BACKLOG_DIR.mkdir(parents=True, exist_ok=True)

    index_rows = []
    for phase in PHASES:
        epics = build_backlog_for_phase(phase)
        (BACKLOG_DIR / phase["file"]).write_text(build_phase_doc(phase, epics), encoding="utf-8")
        total_tickets = sum(len(e["tickets"]) for e in epics)
        index_rows.append(
            f"| `{phase['id']}` | `{phase['file']}` | {len(epics)} | {total_tickets} | `{phase['phase_doc']}` |"
        )

    (BACKLOG_DIR / "README.md").write_text(build_readme("\n".join(index_rows)), encoding="utf-8")

    print(f"Generated {len(PHASES)} backlog files + README in {BACKLOG_DIR}")
    for phase in PHASES:
        print(f"- {phase['file']}")


if __name__ == "__main__":
    main()

