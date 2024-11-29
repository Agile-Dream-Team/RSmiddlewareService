import logging
from app.kafka import kafka_client
from app.shared import (
    messages_sensor_data_response,
    messages_consumed_sensor_data_event,
    lock_sensor_data_response,
    messages_get_all_sensor_data_response,
    messages_consumed_get_all_sensor_data_event,
    lock_get_all_sensor_data_response,
    messages_get_by_id_sensor_data_response,
    messages_consumed_get_by_id_sensor_data_event,
    lock_get_by_id_sensor_data_response,
)


@kafka_client.topic('sensor_data_response')
def consume_message_save_sensor_data(msg):
    """
    Consumes messages from the sensor_data_response topic.
    Handles saving sensor data responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
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
    """
    Consumes messages from the get_all_sensor_data_response topic.
    Handles retrieving all sensor data responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
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
    """
    Consumes messages from the get_by_id_sensor_data_response topic.
    Handles retrieving sensor data by ID responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_by_id_sensor_data_response:
            messages_get_by_id_sensor_data_response.append(msg)
        logging.info(f"Consumed message in get_by_id_response: {msg}")
        messages_consumed_get_by_id_sensor_data_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_response: {e}")
