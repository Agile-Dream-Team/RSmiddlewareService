import logging
from fastapi import APIRouter, Depends
from app.dto.bucket_dto import BucketDTO
from app.responses.custom_responses import ErrorModel
from app.services.bucket_service import BucketService
from RSKafkaWrapper.client import KafkaClient

bucket_router = APIRouter()

response_models = {
    400: {"model": ErrorModel},
    401: {"model": ErrorModel},
    500: {"model": ErrorModel}
}


def get_kafka_client() -> KafkaClient:
    return KafkaClient.instance()


def get_bucket_service(client: KafkaClient = Depends(get_kafka_client)) -> BucketService:
    return BucketService(client)


@bucket_router.post("/")
async def save_bucket(bucket_data: BucketDTO, service: BucketService = Depends(get_bucket_service)):
    received_data = service.save_bucket(bucket_data)
    logging.info(f"Received data: {received_data}")
    return received_data


@bucket_router.get("/")
async def get_all_buckets(service: BucketService = Depends(get_bucket_service)):
    fetch_data = service.get_all_buckets()
    return fetch_data


@bucket_router.get("/{record_id}")
async def get_bucket_by_id(record_id: int, service: BucketService = Depends(get_bucket_service)):
    received_data = service.get_by_id_bucket(record_id)
    logging.info(f"Received data: {received_data}")
    return received_data


@bucket_router.get("/device/{device_id}")
async def get_buckets_by_device(device_id: int, service: BucketService = Depends(get_bucket_service)):
    received_data = service.get_buckets_by_device(device_id)
    logging.info(f"Received data: {received_data}")
    return received_data


@bucket_router.put("/{record_id}")
async def update_bucket(record_id: int, bucket_data: BucketDTO, service: BucketService = Depends(get_bucket_service)):
    received_data = service.update_bucket(record_id, bucket_data)
    logging.info(f"Received data: {received_data}")
    return received_data


@bucket_router.delete("/{record_id}")
async def delete_bucket(record_id: int, service: BucketService = Depends(get_bucket_service)):
    received_data = service.delete_bucket(record_id)
    logging.info(f"Received data: {received_data}")
    return received_data
