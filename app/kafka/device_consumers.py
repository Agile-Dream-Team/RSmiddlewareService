import logging
from app.kafka import kafka_client
from app.shared import (
    messages_device_response,
    messages_consumed_device_event,
    lock_device_response,
    messages_get_all_devices_response,
    messages_consumed_get_all_devices_event,
    lock_get_all_devices_response,
    messages_get_by_id_device_response,
    messages_consumed_get_by_id_device_event,
    lock_get_by_id_device_response,
    messages_update_device_response,
    messages_consumed_update_device_event,
    lock_update_device_response,
    messages_delete_device_response,
    messages_consumed_delete_device_event,
    lock_delete_device_response,
    messages_get_devices_by_pod_response,
    messages_consumed_get_devices_by_pod_event,
    lock_get_devices_by_pod_response, lock_register_device_response, messages_register_device_response,
    messages_consumed_register_device_event, lock_get_active_devices_response, messages_get_active_devices_response,
    messages_consumed_get_active_devices_event,
)


@kafka_client.topic('device_registration_response')
def consume_message_device(msg):
    """
    Consumes messages from the device_response topic.
    Handles device creation responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_register_device_response:
            messages_register_device_response.append(msg)
        logging.info(f"Consumed message in device_registration_response: {msg}")
        messages_consumed_register_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in device_registration_response: {e}")


@kafka_client.topic('get_all_devices_response')
def consume_message_get_all_devices(msg):
    """
    Consumes messages from the get_all_devices_response topic.
    Handles retrieving all devices responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_all_devices_response:
            messages_get_all_devices_response.append(msg)
        logging.info(f"Consumed message in get_all_devices_response: {msg}")
        messages_consumed_get_all_devices_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_devices_response: {e}")


@kafka_client.topic('get_by_id_device_response')
def consume_message_get_by_id_device(msg):
    """
    Consumes messages from the get_by_id_device_response topic.
    Handles retrieving device by ID responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_by_id_device_response:
            messages_get_by_id_device_response.append(msg)
        logging.info(f"Consumed message in get_by_id_device_response: {msg}")
        messages_consumed_get_by_id_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_device_response: {e}")


@kafka_client.topic('get_devices_by_pod_response')
def consume_message_get_devices_by_pod(msg):
    """
    Consumes messages from the get_devices_by_pod_response topic.
    Handles retrieving devices by pod responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_devices_by_pod_response:
            messages_get_devices_by_pod_response.append(msg)
        logging.info(f"Consumed message in get_devices_by_pod_response: {msg}")
        messages_consumed_get_devices_by_pod_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_devices_by_pod_response: {e}")


@kafka_client.topic('update_device_response')
def consume_message_update_device(msg):
    """
    Consumes messages from the update_device_response topic.
    Handles device update responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_update_device_response:
            messages_update_device_response.append(msg)
        logging.info(f"Consumed message in update_device_response: {msg}")
        messages_consumed_update_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in update_device_response: {e}")


@kafka_client.topic('delete_device_response')
def consume_message_delete_device(msg):
    """
    Consumes messages from the delete_device_response topic.
    Handles device deletion responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_delete_device_response:
            messages_delete_device_response.append(msg)
        logging.info(f"Consumed message in delete_device_response: {msg}")
        messages_consumed_delete_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in delete_device_response: {e}")


@kafka_client.topic('get_active_devices_response')
def consume_message_get_active_devices(msg):
    """
    Consumes messages from the delete_device_response topic.
    Handles device deletion responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_active_devices_response:
            messages_get_active_devices_response.append(msg)
        logging.info(f"Consumed message in delete_device_response: {msg}")
        messages_consumed_get_active_devices_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_active_devices_response: {e}")
