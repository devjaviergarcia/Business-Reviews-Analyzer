from __future__ import annotations

from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_service import BusinessService
from src.services.business_query_service import BusinessQueryService
from src.workers.mongo_broker import MongoJobBroker


def create_google_maps_scraper():
    return BusinessService.build_default_scraper()


def create_review_preprocessor() -> ReviewPreprocessor:
    return ReviewPreprocessor()


def create_review_llm_analyzer() -> ReviewLLMAnalyzer:
    return ReviewLLMAnalyzer()


def create_analysis_job_service() -> AnalysisJobService:
    return AnalysisJobService()


def create_worker_job_broker() -> MongoJobBroker:
    return MongoJobBroker(job_service=create_analysis_job_service())


def create_business_query_service() -> BusinessQueryService:
    return BusinessQueryService()


def create_business_service() -> BusinessService:
    return BusinessService(
        scraper=create_google_maps_scraper(),
        preprocessor=create_review_preprocessor(),
        llm_analyzer=create_review_llm_analyzer(),
        job_service=create_analysis_job_service(),
        query_service=create_business_query_service(),
    )
