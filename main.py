import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, status
from pydantic import BaseModel, ValidationError

from app.api.v1.bucket_controller import bucket_router
from app.api.v1.device_controller import device_router
from app.api.v1.pod_controller import pod_router
from app.api.v1.prediction_controller import prediction_router
from app.api.v1.sensor_data_controller import sensor_data_router as sensor_data_router
from app.api.v1.camera_controller import camera_router as camera_router
from app.config.config import Settings
from app.shared import (
    messages_get_all_sensor_data_response,
    messages_consumed_sensor_data_event,
    messages_consumed_camera_event,
    messages_sensor_data_response,
    messages_camera_response,
    messages_get_by_id_sensor_data_response,
    messages_get_all_camera_response,
    messages_get_by_id_camera_response,
    messages_prediction_response,
    messages_consumed_prediction_event,
    messages_consumed_get_by_id_camera_event,
    messages_consumed_get_all_camera_event,
    messages_consumed_get_by_id_sensor_data_event,
    messages_consumed_get_all_sensor_data_event,
    lock_sensor_data_response,
    lock_get_all_sensor_data_response,
    lock_get_by_id_sensor_data_response,
    lock_camera_response,
    lock_get_all_camera_response,
    lock_get_by_id_camera_response,
    lock_prediction_response,
    messages_pod_response,
    messages_get_all_pods_response,
    messages_get_by_id_pod_response,
    messages_delete_pod_response,
    messages_update_pod_response,
    messages_device_response,
    messages_get_all_devices_response,
    messages_get_by_id_device_response,
    messages_delete_device_response,
    messages_update_device_response,
    messages_bucket_response,
    messages_get_all_buckets_response,
    messages_get_by_id_bucket_response,
    messages_consumed_pod_event,
    messages_consumed_get_all_pods_event,
    messages_consumed_get_by_id_pod_event,
    messages_consumed_delete_pod_event,
    messages_consumed_update_pod_event,
    messages_consumed_device_event,
    messages_consumed_get_all_devices_event,
    messages_consumed_get_by_id_device_event,
    messages_consumed_delete_device_event,
    messages_consumed_update_device_event,
    messages_consumed_bucket_event,
    messages_consumed_get_all_buckets_event,
    messages_consumed_get_by_id_bucket_event,
    lock_pod_response,
    lock_get_all_pods_response,
    lock_get_by_id_pod_response,
    lock_delete_pod_response,
    lock_device_response,
    lock_get_all_devices_response,
    lock_get_by_id_device_response,
    lock_update_device_response,
    lock_bucket_response,
    lock_get_all_buckets_response,
    lock_get_by_id_bucket_response,
    lock_update_pod_response, lock_delete_device_response, lock_get_devices_by_pod_response,
    messages_get_devices_by_pod_response, messages_consumed_get_devices_by_pod_event, lock_update_bucket_response,
    messages_update_bucket_response, messages_consumed_update_bucket_event, messages_consumed_delete_bucket_event,
    messages_delete_bucket_response, lock_delete_bucket_response, lock_get_buckets_by_device_response,
    messages_get_buckets_by_device_response, messages_consumed_get_buckets_by_device_event,
)
from RSKafkaWrapper.client import KafkaClient

base_path = '/middleware'


def configure_logging():
    logging.basicConfig(level=logging.INFO)


configure_logging()

app_settings = Settings()
app = FastAPI(docs_url=f'{base_path}/docs')

# Initialize KafkaClient using the singleton pattern
kafka_client = KafkaClient.instance(app_settings.kafka_bootstrap_servers, app_settings.kafka_group_id)


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    try:
        local_settings = Settings()
    except ValidationError as e:
        logging.error(f"Environment variable validation error: {e}")
        raise

    existing_topics = kafka_client.list_topics()
    logging.info(f"Creating Kafka topics: {local_settings.kafka_topics}")
    for topic in local_settings.kafka_topics:
        if topic not in existing_topics:
            kafka_client.create_topic(topic)
        else:
            logging.info(f"Topic '{topic}' already exists.")

    yield


app.router.lifespan_context = lifespan
app.include_router(sensor_data_router, prefix=f"{base_path}/api/v1/sensor_data", tags=["sensor_data"])
app.include_router(camera_router, prefix=f"{base_path}/api/v1/camera", tags=["camera"])
app.include_router(prediction_router, prefix=f"{base_path}/api/v1/prediction", tags=["prediction"])
app.include_router(pod_router, prefix=f"{base_path}/api/v1/pods", tags=["Pods"])
app.include_router(device_router, prefix=f"{base_path}/api/v1/devices", tags=["Devices"])
app.include_router(bucket_router, prefix=f"{base_path}/api/v1/buckets", tags=["Buckets"])


class HealthCheck(BaseModel):
    webhook_status: str = "OK"
    kafka_status: str = "Not implemented"
    msg: str = "Hello world"


@app.get(base_path, response_model=HealthCheck, status_code=status.HTTP_200_OK)
async def get_health() -> HealthCheck:
    logging.info("Health check endpoint called")
    return HealthCheck()


@kafka_client.topic('sensor_data_response')
def consume_message_save_sensor_data(msg):
    try:
        with lock_sensor_data_response:
            messages_sensor_data_response.append(msg)
        logging.info(f"Consumed message in sensor_data_response: {msg}")
        messages_consumed_sensor_data_event.set()
        logging.info("Event set after consuming message.")
    except Exception as e:
        logging.error(f"Error processing message in sensor_data_response: {e}")


@kafka_client.topic('get_all_sensor_data_response')
def consume_message_get_all_sensor_data(msg):
    try:
        with lock_get_all_sensor_data_response:
            messages_get_all_sensor_data_response.append(msg)
        logging.info(f"Consumed message in get_all_sensor_data_response: {msg}")
        messages_consumed_get_all_sensor_data_event.set()
        logging.info("Event set after consuming message.")
    except Exception as e:
        logging.error(f"Error processing message in get_all_sensor_data_response: {e}")


@kafka_client.topic('get_by_id_sensor_data_response')
def consume_message_get_by_id_sensor_data(msg):
    try:
        with lock_get_by_id_sensor_data_response:
            messages_get_by_id_sensor_data_response.append(msg)
        logging.info(f"Consumed message in get_by_id_response: {msg}")
        messages_consumed_get_by_id_sensor_data_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_response: {e}")


@kafka_client.topic('camera_response')
def consume_message_camera(msg):
    try:
        with lock_camera_response:
            messages_camera_response.append(msg)
        logging.info(f"Consumed message in camera_response: {msg}")
        messages_consumed_camera_event.set()
    except Exception as e:
        logging.error(f"Error processing message in camera_response: {e}")


@kafka_client.topic('get_all_camera_response')
def consume_message_get_all_camera(msg):
    try:
        with lock_get_all_camera_response:
            messages_get_all_camera_response.append(msg)
        logging.info(f"Consumed message in get_all_camera_response: {msg}")
        messages_consumed_get_all_camera_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_camera_response: {e}")


@kafka_client.topic('get_by_id_camera_response')
def consume_message_get_by_id_camera(msg):
    try:
        logging.info(f"Received message in get_by_id_camera_response: {msg}")
        with lock_get_by_id_camera_response:
            messages_get_by_id_camera_response.append(msg)
        logging.info(f"Appended message to messages_get_by_id_camera_response: {messages_get_by_id_camera_response}")
        messages_consumed_get_by_id_camera_event.set()
        logging.info("Event set for get_by_id_camera_response")
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_camera_response: {e}")


@kafka_client.topic('prediction_response')
def consume_message_prediction(msg):
    try:
        with lock_prediction_response:
            messages_prediction_response.append(msg)
        logging.info(f"Consumed message in prediction_response: {msg}")
        messages_consumed_prediction_event.set()
    except Exception as e:
        logging.error(f"Error processing message in prediction_response: {e}")


@kafka_client.topic('pod_response')
def consume_message_pod(msg):
    try:
        with lock_pod_response:
            messages_pod_response.append(msg)
        logging.info(f"Consumed message in pod_response: {msg}")
        messages_consumed_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in pod_response: {e}")


@kafka_client.topic('get_all_pods_response')
def consume_message_get_all_pods(msg):
    try:
        with lock_get_all_pods_response:
            messages_get_all_pods_response.append(msg)
        logging.info(f"Consumed message in get_all_pods_response: {msg}")
        messages_consumed_get_all_pods_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_pods_response: {e}")


@kafka_client.topic('update_pod_response')
def consume_message_update_pod(msg):
    try:
        with lock_update_pod_response:
            messages_update_pod_response.append(msg)
        logging.info(f"Consumed message in update_pod_response: {msg}")
        messages_consumed_update_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in update_pod_response: {e}")


@kafka_client.topic('delete_pod_response')
def consume_message_delete_pod(msg):
    try:
        with lock_delete_pod_response:
            messages_delete_pod_response.append(msg)
        logging.info(f"Consumed message in get_all_pods_response: {msg}")
        messages_consumed_delete_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_pods_response: {e}")


@kafka_client.topic('get_by_id_pod_response')
def consume_message_get_by_id_pod(msg):
    try:
        with lock_get_by_id_pod_response:
            messages_get_by_id_pod_response.append(msg)
        logging.info(f"Consumed message in get_by_id_pod_response: {msg}")
        messages_consumed_get_by_id_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_pod_response: {e}")


@kafka_client.topic('device_response')
def consume_message_device(msg):
    try:
        with lock_device_response:
            messages_device_response.append(msg)
        logging.info(f"Consumed message in device_response: {msg}")
        messages_consumed_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in device_response: {e}")


@kafka_client.topic('get_all_devices_response')
def consume_message_get_all_devices(msg):
    try:
        with lock_get_all_devices_response:
            messages_get_all_devices_response.append(msg)
        logging.info(f"Consumed message in get_all_devices_response: {msg}")
        messages_consumed_get_all_devices_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_devices_response: {e}")


@kafka_client.topic('get_by_id_device_response')
def consume_message_get_by_id_device(msg):
    try:
        with lock_get_by_id_device_response:
            messages_get_by_id_device_response.append(msg)
        logging.info(f"Consumed message in get_by_id_device_response: {msg}")
        messages_consumed_get_by_id_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_device_response: {e}")


@kafka_client.topic('get_devices_by_pod_response')
def consume_message_get_devices_by_pod(msg):
    try:
        with lock_get_devices_by_pod_response:
            messages_get_devices_by_pod_response.append(msg)
        logging.info(f"Consumed message in get_by_id_device_response: {msg}")
        messages_consumed_get_devices_by_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_device_response: {e}")


@kafka_client.topic('update_device_response')
def consume_message_update_device(msg):
    try:
        with lock_update_device_response:
            messages_update_device_response.append(msg)
        logging.info(f"Consumed message in update_device_response: {msg}")
        messages_consumed_update_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in update_device_response: {e}")


@kafka_client.topic('delete_device_response')
def consume_message_delete_device(msg):
    try:
        with lock_delete_device_response:
            messages_delete_device_response.append(msg)
        logging.info(f"Consumed message in delete_device_response: {msg}")
        messages_consumed_delete_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in delete_device_response: {e}")


@kafka_client.topic('bucket_response')
def consume_message_bucket(msg):
    try:
        with lock_bucket_response:
            messages_bucket_response.append(msg)
        logging.info(f"Consumed message in bucket_response: {msg}")
        messages_consumed_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in bucket_response: {e}")


@kafka_client.topic('get_all_buckets_response')
def consume_message_get_all_buckets(msg):
    try:
        with lock_get_all_buckets_response:
            messages_get_all_buckets_response.append(msg)
        logging.info(f"Consumed message in get_all_buckets_response: {msg}")
        messages_consumed_get_all_buckets_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_buckets_response: {e}")


@kafka_client.topic('get_by_id_bucket_response')
def consume_message_get_by_id_bucket(msg):
    try:
        with lock_get_by_id_bucket_response:
            messages_get_by_id_bucket_response.append(msg)
        logging.info(f"Consumed message in get_by_id_bucket_response: {msg}")
        messages_consumed_get_by_id_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_bucket_response: {e}")


@kafka_client.topic('update_bucket_response')
def consume_message_update_bucket(msg):
    try:
        with lock_update_bucket_response:
            messages_update_bucket_response.append(msg)
        logging.info(f"Consumed message in update_bucket_response: {msg}")
        messages_consumed_update_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in update_bucket_response: {e}")


@kafka_client.topic('delete_bucket_response')
def consume_message_delete_bucket(msg):
    try:
        with lock_delete_bucket_response:
            messages_delete_bucket_response.append(msg)
        logging.info(f"Consumed message in delete_bucket_response: {msg}")
        messages_consumed_delete_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in delete_bucket_response: {e}")


@kafka_client.topic('get_buckets_by_device_response')
def consume_message_delete_get_buckets_by_device(msg):
    try:
        with lock_get_buckets_by_device_response:
            messages_get_buckets_by_device_response.append(msg)
        logging.info(f"Consumed message in get_buckets_by_device_response: {msg}")
        messages_consumed_get_buckets_by_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_buckets_by_device_response: {e}")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=app_settings.webhook_host,
        port=app_settings.webhook_port,
        reload=app_settings.environment == 'dev'
    )
