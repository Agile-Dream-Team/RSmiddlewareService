import logging
from fastapi import APIRouter, HTTPException, Depends
from app.dto.device_dto import DeviceDTO
from app.responses.custom_responses import SuccessModel, ErrorModel
from app.services.device_service import DeviceService
from RSKafkaWrapper.client import KafkaClient

device_router = APIRouter()

response_models = {
    400: {"model": ErrorModel},
    401: {"model": ErrorModel},
    500: {"model": ErrorModel}
}


def get_kafka_client() -> KafkaClient:
    return KafkaClient.instance()


def get_device_service(client: KafkaClient = Depends(get_kafka_client)) -> DeviceService:
    return DeviceService(client)


@device_router.post("/")
async def save_device(device_data: DeviceDTO, service: DeviceService = Depends(get_device_service)):
    received_data = service.save_device(device_data)
    logging.info(f"Received data: {received_data}")
    return received_data


@device_router.get("/")
async def get_all_devices(service: DeviceService = Depends(get_device_service)):
    fetch_data = service.get_all_devices()
    return fetch_data


@device_router.get("/{record_id}")
async def get_device_by_id(record_id: int, service: DeviceService = Depends(get_device_service)):
    received_data = service.get_by_id_device(record_id)
    logging.info(f"Received data: {received_data}")
    return received_data


@device_router.get("/pod/{pod_id}")
async def get_devices_by_pod(pod_id: int, service: DeviceService = Depends(get_device_service)):
    received_data = service.get_devices_by_pod(pod_id)
    logging.info(f"Received data: {received_data}")
    return received_data


@device_router.patch("/{record_id}")
async def update_device(record_id: int, device_data: DeviceDTO, service: DeviceService = Depends(get_device_service)):
    received_data = service.update_device(record_id, device_data)
    logging.info(f"Received data: {received_data}")
    return received_data


@device_router.delete("/{record_id}")
async def delete_device(record_id: int, service: DeviceService = Depends(get_device_service)):
    received_data = service.delete_device(record_id)
    logging.info(f"Received data: {received_data}")
    return received_data
