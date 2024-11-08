import logging
from fastapi import APIRouter, Depends
from app.dto.pod_dto import PodDTO
from app.responses.custom_responses import SuccessModel, ErrorModel
from app.services.pod_service import PodService
from RSKafkaWrapper.client import KafkaClient

pod_router = APIRouter()

response_models = {
    400: {"model": ErrorModel},
    401: {"model": ErrorModel},
    500: {"model": ErrorModel}
}


def get_kafka_client() -> KafkaClient:
    return KafkaClient.instance()


def get_pod_service(client: KafkaClient = Depends(get_kafka_client)) -> PodService:
    return PodService(client)


@pod_router.post("/")
async def save_pod(pod_data: PodDTO, service: PodService = Depends(get_pod_service)):
    received_data = service.save_pod(pod_data)
    logging.info(f"Received data: {received_data}")
    return received_data


@pod_router.get("/")
async def get_all_pods(service: PodService = Depends(get_pod_service)):
    fetch_data = service.get_all_pods()
    return fetch_data


@pod_router.get("/{record_id}")
async def get_pod_by_id(record_id: int, service: PodService = Depends(get_pod_service)):
    received_data = service.get_by_id_pod(record_id)
    logging.info(f"Received data: {received_data}")
    return received_data


@pod_router.patch("/{record_id}")
async def update_pod(record_id: int, pod_data: PodDTO, service: PodService = Depends(get_pod_service)):
    received_data = service.update_pod(record_id, pod_data)
    logging.info(f"Received data: {received_data}")
    return received_data


@pod_router.delete("/{record_id}")
async def delete_pod(record_id: int, service: PodService = Depends(get_pod_service)):
    received_data = service.delete_pod(record_id)
    logging.info(f"Received data: {received_data}")
    return received_data
