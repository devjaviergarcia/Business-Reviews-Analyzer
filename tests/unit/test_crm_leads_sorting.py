from __future__ import annotations

from src.crm.repositories.mongo import MongoLeadRepository
from src.services.crm_service import CRMService


class _Dummy:
    pass


def _service() -> CRMService:
    service = CRMService(job_service=_Dummy(), business_service=_Dummy())
    service._use_repo_v2 = False
    service._use_discovery_v2 = False
    return service


def test_service_resolve_leads_sort_supports_extended_fields() -> None:
    service = _service()
    expected = {
        "updated_at": "updated_at",
        "business_name": "business_name_normalized",
        "score": "score",
        "status": "status",
        "consent_status": "legal.consent_status",
        "source": "source",
    }
    for sort_by, field_name in expected.items():
        spec = service._resolve_leads_sort(sort_by=sort_by, sort_dir="asc")
        assert spec[0] == (field_name, 1)


def test_repository_resolve_sort_supports_extended_fields() -> None:
    repo = MongoLeadRepository()
    expected = {
        "updated_at": "updated_at",
        "business_name": "business_name_normalized",
        "score": "score",
        "status": "status",
        "consent_status": "legal.consent_status",
        "source": "source",
    }
    for sort_by, field_name in expected.items():
        spec = repo._resolve_sort(sort_by=sort_by, sort_dir="desc")
        assert spec[0] == (field_name, -1)

