import logging
from app.kafka import kafka_client
from app.shared import (
    messages_camera_response,
    messages_consumed_camera_event,
    lock_camera_response,
    messages_get_all_camera_response,
    messages_consumed_get_all_camera_event,
    lock_get_all_camera_response,
    messages_get_by_id_camera_response,
    messages_consumed_get_by_id_camera_event,
    lock_get_by_id_camera_response,
)


@kafka_client.topic('camera_response')
def consume_message_camera(msg):
    """
    Consumes messages from the camera_response topic.
    Handles camera data responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_camera_response:
            messages_camera_response.append(msg)
        logging.info(f"Consumed message in camera_response: {msg}")
        messages_consumed_camera_event.set()
    except Exception as e:
        logging.error(f"Error processing message in camera_response: {e}")


@kafka_client.topic('get_all_camera_response')
def consume_message_get_all_camera(msg):
    """
    Consumes messages from the get_all_camera_response topic.
    Handles retrieving all camera data responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_all_camera_response:
            messages_get_all_camera_response.append(msg)
        logging.info(f"Consumed message in get_all_camera_response: {msg}")
        messages_consumed_get_all_camera_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_camera_response: {e}")


@kafka_client.topic('get_by_id_camera_response')
def consume_message_get_by_id_camera(msg):
    """
    Consumes messages from the get_by_id_camera_response topic.
    Handles retrieving camera data by ID responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        logging.info(f"Received message in get_by_id_camera_response: {msg}")
        with lock_get_by_id_camera_response:
            messages_get_by_id_camera_response.append(msg)
        logging.info(f"Appended message to messages_get_by_id_camera_response: {messages_get_by_id_camera_response}")
        messages_consumed_get_by_id_camera_event.set()
        logging.info("Event set for get_by_id_camera_response")
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_camera_response: {e}")
