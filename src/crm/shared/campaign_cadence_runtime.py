from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from bson import ObjectId
from bson.errors import InvalidId

from src.database import get_database
from src.models.crm import CRMCadenceStep, CRMCadenceTemplate


DatabaseFactory = Callable[[], Any]
NowUtcFn = Callable[[], datetime]


class CampaignCadenceRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        cadence_collection_name: str,
        default_cadence_key: str,
        now_utc: NowUtcFn,
    ) -> None:
        self._database_factory = database_factory
        self._cadence_collection_name = cadence_collection_name
        self._default_cadence_key = default_cadence_key
        self._now_utc = now_utc

    async def resolve_cadence_template(self, cadence_template_id: str | None) -> dict[str, Any]:
        await self.ensure_default_cadence_template()
        cadence = self._database_factory()[self._cadence_collection_name]
        normalized_id = str(cadence_template_id or "").strip()
        if normalized_id:
            try:
                doc = await cadence.find_one({"_id": ObjectId(normalized_id)})
            except InvalidId:
                doc = await cadence.find_one({"key": normalized_id})
            if doc is not None:
                return doc

        fallback = await cadence.find_one({"key": self._default_cadence_key})
        if fallback is None:
            raise RuntimeError("Default cadence template is missing.")
        return fallback

    async def ensure_default_cadence_template(self) -> None:
        cadence = self._database_factory()[self._cadence_collection_name]
        now = self._now_utc()
        default_steps = [
            CRMCadenceStep(
                step_order=1,
                step_key="d0_intro",
                delay_days=0,
                subject_template="{business_name}: te comparto un mini informe de reputación",
                body_template=(
                    "Hola,\n\n"
                    "Hemos revisado la reputación online de {business_name}.\n"
                    "Resumen rápido:\n"
                    "{mini_report}\n\n"
                    "Si te encaja, te enseño en 15 minutos cómo mejorar estos puntos.\n"
                    "{cta_url}\n\n"
                    "Si no quieres recibir más mensajes, puedes darte de baja aquí: {unsubscribe_url}\n"
                ),
            ),
            CRMCadenceStep(
                step_order=2,
                step_key="d3_recordatorio",
                delay_days=3,
                subject_template="{business_name}: un dato clave para mejorar tu reputación",
                body_template=(
                    "Hola de nuevo,\n\n"
                    "Te comparto un insight adicional de {business_name}:\n"
                    "{mini_report}\n\n"
                    "Si quieres, te lo explico en una demo corta:\n"
                    "{cta_url}\n\n"
                    "Baja automática: {unsubscribe_url}\n"
                ),
            ),
            CRMCadenceStep(
                step_order=3,
                step_key="d7_cierre",
                delay_days=7,
                subject_template="Cierro hilo: {business_name}",
                body_template=(
                    "Último mensaje por aquí, prometido.\n\n"
                    "Si en otro momento quieres revisar el informe de {business_name},"
                    " aquí tienes acceso directo:\n"
                    "{cta_url}\n\n"
                    "Puedes darte de baja aquí: {unsubscribe_url}\n"
                ),
            ),
        ]
        template = CRMCadenceTemplate(
            key=self._default_cadence_key,
            name="Cadencia opt-in 3 toques (D0/D+3/D+7)",
            locale="es-ES",
            is_default=True,
            steps=default_steps,
            created_at=now,
            updated_at=now,
        )
        payload = template.model_dump(mode="python")
        await cadence.update_one(
            {"key": self._default_cadence_key},
            {
                "$set": {
                    "name": payload["name"],
                    "locale": payload["locale"],
                    "is_default": True,
                    "steps": payload["steps"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "key": self._default_cadence_key,
                },
            },
            upsert=True,
        )
