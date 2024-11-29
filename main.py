import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, status
from pydantic import BaseModel, ValidationError

from app.api.v1.bucket_controller import bucket_router
from app.api.v1.device_registry_controller import command_router
from app.api.v1.device_controller import device_router
from app.api.v1.pod_controller import pod_router
from app.api.v1.prediction_controller import prediction_router
from app.api.v1.sensor_data_controller import sensor_data_router
from app.api.v1.camera_controller import camera_router
from app.config.config import Settings
from app.kafka import kafka_client

# Import all Kafka consumers
from app.kafka.sensor_consumers import *
from app.kafka.camera_consumers import *
from app.kafka.prediction_consumers import *
from app.kafka.pod_consumers import *
from app.kafka.device_consumers import *
from app.kafka.bucket_consumers import *

base_path = '/middleware'

app_settings = Settings()
app = FastAPI(docs_url=f'{base_path}/docs')


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles Kafka topic creation during startup.
    """
    try:
        local_settings = Settings()

        # Create Kafka topics if they don't exist
        existing_topics = kafka_client.list_topics()
        logging.info(f"Creating Kafka topics: {local_settings.kafka_topics}")

        for topic in local_settings.kafka_topics:
            if topic not in existing_topics:
                kafka_client.create_topic(topic)
            else:
                logging.info(f"Topic '{topic}' already exists.")
    except ValidationError as e:
        logging.error(f"Environment variable validation error: {e}")
        raise

    yield


# Configure lifespan and routers
app.router.lifespan_context = lifespan

# Include all routers with their respective prefixes
app.include_router(
    sensor_data_router,
    prefix=f"{base_path}/api/v1/sensor_data",
    tags=["sensor_data"]
)
app.include_router(
    camera_router,
    prefix=f"{base_path}/api/v1/camera",
    tags=["camera"]
)
app.include_router(
    prediction_router,
    prefix=f"{base_path}/api/v1/prediction",
    tags=["prediction"]
)
app.include_router(
    pod_router,
    prefix=f"{base_path}/api/v1/pods",
    tags=["Pods"]
)
app.include_router(
    device_router,
    prefix=f"{base_path}/api/v1/devices",
    tags=["Devices"]
)
app.include_router(
    bucket_router,
    prefix=f"{base_path}/api/v1/buckets",
    tags=["Buckets"]
)
"""
app.include_router(
    command_router,
    prefix=f"{base_path}/api/v1/commands",
    tags=["Commands"]
)
"""


class HealthCheck(BaseModel):
    """Health check response model"""
    webhook_status: str = "OK"
    kafka_status: str = "Not implemented"
    msg: str = "Hello world"


@app.get(base_path, response_model=HealthCheck, status_code=status.HTTP_200_OK)
async def get_health() -> HealthCheck:
    """Health check endpoint"""
    logging.info("Health check endpoint called")
    return HealthCheck()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=app_settings.webhook_host,
        port=app_settings.webhook_port,
        reload=app_settings.environment == 'dev'
    )
