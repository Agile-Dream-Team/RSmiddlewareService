import logging
from RSKafkaWrapper.client import KafkaClient
from app.shared import (
    messages_bucket_response,
    messages_get_all_buckets_response,
    messages_get_by_id_bucket_response,
    messages_consumed_bucket_event,
    lock_bucket_response,
    lock_get_all_buckets_response,
    lock_get_by_id_bucket_response,
    messages_consumed_get_by_id_bucket_event, lock_update_bucket_response, messages_update_bucket_response,
    messages_consumed_update_bucket_event, lock_delete_bucket_response, messages_delete_bucket_response,
    messages_consumed_delete_bucket_event, lock_get_buckets_by_device_response, messages_get_buckets_by_device_response,
    messages_consumed_get_buckets_by_device_event
)
from app.api.utils import parse_and_flatten_messages
from app.mapper.bucket_mapper import BucketMapper, UpdateBucketMapper


class BucketService:
    def __init__(self, client: KafkaClient):
        self.client = client

    def save_bucket(self, bucket_dto):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_bucket_response:
                messages_bucket_response.clear()
            messages_consumed_bucket_event.clear()

            bucket_mapper = BucketMapper(
                serial=bucket_dto.serial,
                name=bucket_dto.name,
                description=bucket_dto.description,
                device_id=bucket_dto.device_id,
                user_id=bucket_dto.user_id
            )

            logging.info(f"Sending message: {bucket_mapper.model_dump()}")
            self.client.send_message("bucket", bucket_mapper.model_dump())

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_bucket_event.wait(timeout=10)

            with lock_bucket_response:
                response = parse_and_flatten_messages(messages_bucket_response)
            logging.info(f"Received data SAVE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while saving bucket: {e}")
            raise

    def get_all_buckets(self):
        try:
            logging.info("Clearing previous messages and events.")
            with lock_get_all_buckets_response:
                messages_get_all_buckets_response.clear()
            messages_consumed_bucket_event.clear()

            to_send = {"event": "get_all"}
            logging.info(f"Sending message: {to_send}")
            self.client.send_message("get_all_buckets", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_bucket_event.wait(timeout=10)

            with lock_get_all_buckets_response:
                response = parse_and_flatten_messages(messages_get_all_buckets_response)
            logging.info(f"Received data GET ALL: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while fetching all buckets: {e}")
            raise

    def get_by_id_bucket(self, record_id: int):
        try:
            with lock_get_by_id_bucket_response:
                messages_get_by_id_bucket_response.clear()
            messages_consumed_get_by_id_bucket_event.clear()

            to_send = {
                "event": "get_by_id",
                "id": record_id
            }
            self.client.send_message("get_by_id_bucket", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_get_by_id_bucket_event.wait(timeout=10)

            with lock_get_by_id_bucket_response:
                response = parse_and_flatten_messages(messages_get_by_id_bucket_response)
            logging.info(f"Received data bucket: {response}")
            return response

        except Exception as e:
            logging.error(f"Error in get_by_id_bucket: {e}")
            return None

    def get_buckets_by_device(self, device_id: int):
        try:
            logging.info("Clearing previous messages and events.")
            with lock_get_buckets_by_device_response:
                messages_get_buckets_by_device_response.clear()
            messages_consumed_get_buckets_by_device_event.clear()

            to_send = {
                "event": "get_buckets_by_device",
                "device_id": device_id
            }
            logging.info(f"Sending message: {to_send}")
            self.client.send_message("get_buckets_by_device", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_get_buckets_by_device_event.wait(timeout=10)

            with lock_get_buckets_by_device_response:
                response = parse_and_flatten_messages(messages_get_buckets_by_device_response)
            logging.info(f"Received data GET BUCKETS BY DEVICE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while fetching buckets by device: {e}")
            raise

    def update_bucket(self, record_id, bucket_dto):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_update_bucket_response:
                messages_update_bucket_response.clear()
            messages_consumed_update_bucket_event.clear()

            update_mapper = UpdateBucketMapper(id=record_id)
            update_mapper.update_from_dict(bucket_dto.model_dump())

            to_send = {
                "event": "update",
                "data": update_mapper.model_dump_not_none()
            }

            logging.info(f"Sending message: {to_send}")
            self.client.send_message("update_bucket", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_bucket_event.wait(timeout=10)

            with lock_update_bucket_response:
                response = parse_and_flatten_messages(messages_update_bucket_response)
            logging.info(f"Received data UPDATE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while updating bucket: {e}")
            raise

    def delete_bucket(self, record_id: int):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_delete_bucket_response:
                messages_delete_bucket_response.clear()
            messages_consumed_delete_bucket_event.clear()

            to_send = {
                "event": "delete",
                "id": record_id
            }

            logging.info(f"Sending message: {to_send}")
            self.client.send_message("delete_bucket", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_delete_bucket_event.wait(timeout=10)

            with lock_delete_bucket_response:
                response = parse_and_flatten_messages(messages_delete_bucket_response)
            logging.info(f"Received data DELETE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while deleting bucket: {e}")
            raise

