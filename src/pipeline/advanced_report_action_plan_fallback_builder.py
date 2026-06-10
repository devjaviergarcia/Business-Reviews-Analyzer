from __future__ import annotations

from typing import Any, Callable


CONCRETE_ACTIONS = {
    "servicio": {
        "inmediato": (
            "Asignar una persona de sala para revisar cada mesa cada 10-12 minutos en horas punta "
            "y actuar cuando una mesa lleve más de 10 minutos sin atención."
        ),
        "medio": (
            "Implantar un protocolo de atención por tiempos (saludo, toma de pedido, seguimiento) "
            "y repasarlo con el equipo en una formación breve semanal."
        ),
        "largo": (
            "Crear una revisión semanal de reseñas de servicio con incidencias repetidas "
            "y un registro de mejoras aplicadas."
        ),
    },
    "tiempo de espera": {
        "inmediato": (
            "Controlar el tiempo de salida del primer plato y avisar a sala si supera 18 minutos "
            "para informar al cliente antes de que se genere frustración."
        ),
        "medio": (
            "Separar en cocina los platos de preparación rápida y lenta para mejorar el flujo "
            "en horas de mayor demanda."
        ),
        "largo": (
            "Revisar mensualmente tiempos por franja horaria y ajustar turnos o carta en horas pico."
        ),
    },
    "calidad de la comida": {
        "inmediato": (
            "Validar temperatura y presentación antes de sacar cada plato en los servicios con más afluencia."
        ),
        "medio": (
            "Revisar recetas y puntos críticos de los platos más señalados en reseñas negativas "
            "con una cata interna del equipo."
        ),
        "largo": (
            "Definir fichas de referencia para los platos clave (presentación, tiempos y estándar mínimo)."
        ),
    },
    "relación calidad-precio": {
        "inmediato": (
            "Revisar tres platos con más quejas de precio y ajustar ración, presentación o comunicación del valor."
        ),
        "medio": (
            "Crear una propuesta de valor clara en carta para que el cliente entienda qué incluye y por qué cuesta eso."
        ),
        "largo": (
            "Comparar trimestralmente precios y percepción frente a la competencia local para mantener equilibrio."
        ),
    },
}


def build_action_plan_fallback(
    *,
    top_problems: list[dict[str, Any]],
    customer_clusters: dict[str, Any],
    generic_comment_problem: str,
    friendly_problem_label: Callable[[str], str],
    safe_float: Callable[[Any, float], float],
    safe_int: Callable[[Any, int], int],
    infer_action_type: Callable[[str], str],
    infer_action_tool: Callable[[str], str],
) -> dict[str, Any]:
    immediate: list[dict[str, Any]] = []
    medium: list[dict[str, Any]] = []
    long_term: list[dict[str, Any]] = []

    for problem in top_problems:
        label_raw = str(problem.get("problem", generic_comment_problem) or generic_comment_problem)
        label = friendly_problem_label(label_raw)
        severity = safe_float(problem.get("severity"), 0.0)
        impact = "alto" if severity >= 0.65 else "medio"
        owner_short = "Encargado o responsable de turno"
        owner_medium = "Gerencia y responsable del área"
        owner_long = "Dirección del negocio"

        key = ""
        for candidate in CONCRETE_ACTIONS.keys():
            if candidate in label.lower():
                key = candidate
                break

        if key:
            immediate_action = CONCRETE_ACTIONS[key]["inmediato"]
            medium_action = CONCRETE_ACTIONS[key]["medio"]
            long_action = CONCRETE_ACTIONS[key]["largo"]
        else:
            immediate_action = (
                f"Identificar los tres fallos más repetidos de '{label}' esta semana y aplicar una corrección "
                "simple antes del próximo fin de semana."
            )
            medium_action = (
                f"Ordenar el proceso interno de '{label}' en pasos claros y explicarlo al equipo en una sesión breve."
            )
            long_action = (
                f"Revisar cada mes las menciones de '{label}' y ajustar la operativa para evitar repeticiones."
            )

        immediate.append(
            {
                "problema": label,
                "accion": immediate_action,
                "action": immediate_action,
                "por_que": "Es un foco recurrente en reseñas recientes y está dañando la experiencia.",
                "impact": impact,
                "encargado": owner_short,
                "owner": owner_short,
                "plazo_dias": 14,
                "horizon_days": 14,
                "indicador": f"Reducir en un 25% las menciones negativas sobre {label}.",
                "kpi": f"Reducir en un 25% las menciones negativas sobre {label}.",
                "objetivo": f"Reducir en un 25% las menciones negativas sobre {label}.",
                "tipo": infer_action_type(f"{label} {immediate_action}"),
                "herramienta_si_aplica": infer_action_tool(immediate_action),
            }
        )
        medium.append(
            {
                "problema": label,
                "accion": medium_action,
                "action": medium_action,
                "por_que": "Un proceso claro reduce errores repetidos y mejora consistencia.",
                "impact": impact,
                "encargado": owner_medium,
                "owner": owner_medium,
                "plazo_dias": 60,
                "horizon_days": 60,
                "indicador": f"Subir al menos 0.2 puntos la satisfacción ligada a {label}.",
                "kpi": f"Subir al menos 0.2 puntos la satisfacción ligada a {label}.",
                "objetivo": f"Subir al menos 0.2 puntos la satisfacción ligada a {label}.",
                "tipo": infer_action_type(f"{label} {medium_action}"),
                "herramienta_si_aplica": infer_action_tool(medium_action),
            }
        )
        long_term.append(
            {
                "problema": label,
                "accion": long_action,
                "action": long_action,
                "por_que": "Convertirlo en hábito evita recaídas y protege la reputación a largo plazo.",
                "impact": "alto",
                "encargado": owner_long,
                "owner": owner_long,
                "plazo_dias": 120,
                "horizon_days": 120,
                "indicador": "Mantener un control activo que detecte incidencias antes de que escalen.",
                "kpi": "Mantener un control activo que detecte incidencias antes de que escalen.",
                "objetivo": "Mantener un control activo que detecte incidencias antes de que escalen.",
                "tipo": infer_action_type(f"{label} {long_action}"),
                "herramienta_si_aplica": infer_action_tool(long_action),
            }
        )

    if not immediate:
        default_action = "Revisar y responder reseñas críticas cada día en menos de 48 horas con respuesta personalizada."
        immediate.append(
            {
                "problema": "gestión de reseñas",
                "accion": default_action,
                "action": default_action,
                "por_que": "Si no se responde rápido, el problema se hace más visible y se repite.",
                "impact": "medio",
                "encargado": "Gerencia del negocio",
                "owner": "Gerencia del negocio",
                "plazo_dias": 14,
                "horizon_days": 14,
                "indicador": "Responder el 100% de reseñas críticas en menos de 48 horas.",
                "kpi": "Responder el 100% de reseñas críticas en menos de 48 horas.",
                "objetivo": "Responder el 100% de reseñas críticas en menos de 48 horas.",
                "tipo": "proceso",
                "herramienta_si_aplica": "Plantilla breve de respuesta y panel de reseñas",
            }
        )

    cluster_count = safe_int(customer_clusters.get("cluster_count"), 0)
    return {
        "inmediato_0_30_dias": immediate[:5],
        "medio_30_90_dias": medium[:5],
        "largo_90_mas_dias": long_term[:5],
        "notes": [
            f"Se detectaron {cluster_count} segmentos de clientes para personalizar acciones.",
        ],
        "llm_generated": False,
    }
