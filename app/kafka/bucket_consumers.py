import logging
from app.kafka import kafka_client
from app.shared import (
    messages_bucket_response,
    messages_consumed_bucket_event,
    lock_bucket_response,
    messages_get_all_buckets_response,
    messages_consumed_get_all_buckets_event,
    lock_get_all_buckets_response,
    messages_get_by_id_bucket_response,
    messages_consumed_get_by_id_bucket_event,
    lock_get_by_id_bucket_response,
    messages_update_bucket_response,
    messages_consumed_update_bucket_event,
    lock_update_bucket_response,
    messages_delete_bucket_response,
    messages_consumed_delete_bucket_event,
    lock_delete_bucket_response,
    messages_get_buckets_by_device_response,
    messages_consumed_get_buckets_by_device_event,
    lock_get_buckets_by_device_response,
)


@kafka_client.topic('bucket_response')
def consume_message_bucket(msg):
    """
    Consumes messages from the bucket_response topic.
    Handles bucket creation responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_bucket_response:
            messages_bucket_response.append(msg)
        logging.info(f"Consumed message in bucket_response: {msg}")
        messages_consumed_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in bucket_response: {e}")


@kafka_client.topic('get_all_buckets_response')
def consume_message_get_all_buckets(msg):
    """
    Consumes messages from the get_all_buckets_response topic.
    Handles retrieving all buckets responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_all_buckets_response:
            messages_get_all_buckets_response.append(msg)
        logging.info(f"Consumed message in get_all_buckets_response: {msg}")
        messages_consumed_get_all_buckets_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_all_buckets_response: {e}")


@kafka_client.topic('get_by_id_bucket_response')
def consume_message_get_by_id_bucket(msg):
    """
    Consumes messages from the get_by_id_bucket_response topic.
    Handles retrieving bucket by ID responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_by_id_bucket_response:
            messages_get_by_id_bucket_response.append(msg)
        logging.info(f"Consumed message in get_by_id_bucket_response: {msg}")
        messages_consumed_get_by_id_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_bucket_response: {e}")


@kafka_client.topic('update_bucket_response')
def consume_message_update_bucket(msg):
    """
    Consumes messages from the update_bucket_response topic.
    Handles bucket update responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_update_bucket_response:
            messages_update_bucket_response.append(msg)
        logging.info(f"Consumed message in update_bucket_response: {msg}")
        messages_consumed_update_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in update_bucket_response: {e}")


@kafka_client.topic('delete_bucket_response')
def consume_message_delete_bucket(msg):
    """
    Consumes messages from the delete_bucket_response topic.
    Handles bucket deletion responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_delete_bucket_response:
            messages_delete_bucket_response.append(msg)
        logging.info(f"Consumed message in delete_bucket_response: {msg}")
        messages_consumed_delete_bucket_event.set()
    except Exception as e:
        logging.error(f"Error processing message in delete_bucket_response: {e}")


@kafka_client.topic('get_buckets_by_device_response')
def consume_message_get_buckets_by_device(msg):
    """
    Consumes messages from the get_buckets_by_device_response topic.
    Handles retrieving buckets by device responses and triggers related events.

    Args:
        msg: The message received from Kafka
    """
    try:
        with lock_get_buckets_by_device_response:
            messages_get_buckets_by_device_response.append(msg)
        logging.info(f"Consumed message in get_buckets_by_device_response: {msg}")
        messages_consumed_get_buckets_by_device_event.set()
    except Exception as e:
        logging.error(f"Error processing message in get_buckets_by_device_response: {e}")
