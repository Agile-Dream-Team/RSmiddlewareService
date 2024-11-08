import logging
from RSKafkaWrapper.client import KafkaClient
from app.shared import (
    messages_device_response,
    messages_get_all_devices_response,
    messages_get_by_id_device_response,
    messages_consumed_device_event,
    lock_device_response,
    lock_get_all_devices_response,
    lock_get_by_id_device_response,
    messages_consumed_get_by_id_device_event, lock_update_device_response, messages_update_device_response,
    messages_consumed_update_device_event, lock_delete_device_response, messages_delete_device_response,
    messages_consumed_delete_device_event, lock_get_devices_by_pod_response, messages_get_devices_by_pod_response,
    messages_consumed_get_devices_by_pod_event
)
from app.api.utils import parse_and_flatten_messages
from app.mapper.device_mapper import DeviceMapper, UpdateDeviceMapper


class DeviceService:
    def __init__(self, client: KafkaClient):
        self.client = client

    def save_device(self, device_dto):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_device_response:
                messages_device_response.clear()
            messages_consumed_device_event.clear()

            device_mapper = DeviceMapper(
                pod_id=device_dto.pod_id,
                serial=device_dto.serial,
                name=device_dto.name,
                description=device_dto.description,
                user_id=device_dto.user_id
            )

            logging.info(f"Sending message: {device_mapper.model_dump()}")
            self.client.send_message("device", device_mapper.model_dump())

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_device_event.wait(timeout=10)

            with lock_device_response:
                response = parse_and_flatten_messages(messages_device_response)
            logging.info(f"Received data SAVE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while saving device: {e}")
            raise

    def get_all_devices(self):
        try:
            logging.info("Clearing previous messages and events.")
            with lock_get_all_devices_response:
                messages_get_all_devices_response.clear()
            messages_consumed_device_event.clear()

            to_send = {"event": "get_all"}
            logging.info(f"Sending message: {to_send}")
            self.client.send_message("get_all_devices", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_device_event.wait(timeout=10)
            with lock_get_all_devices_response:
                response = parse_and_flatten_messages(messages_get_all_devices_response)
            logging.info(f"Received data GET ALL: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while fetching all devices: {e}")
            raise

    def get_by_id_device(self, record_id: int):
        try:
            with lock_get_by_id_device_response:
                messages_get_by_id_device_response.clear()
            messages_consumed_get_by_id_device_event.clear()

            to_send = {
                "event": "get_by_id",
                "id": record_id
            }
            self.client.send_message("get_by_id_device", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_get_by_id_device_event.wait(timeout=10)

            with lock_get_by_id_device_response:
                response = parse_and_flatten_messages(messages_get_by_id_device_response)
            logging.info(f"Received data device: {response}")
            return response

        except Exception as e:
            logging.error(f"Error in get_by_id_device: {e}")
            return None

    def get_devices_by_pod(self, pod_id: int):
        try:
            logging.info("Clearing previous messages and events.")
            with lock_get_devices_by_pod_response:
                messages_get_devices_by_pod_response.clear()
            messages_consumed_get_devices_by_pod_event.clear()

            to_send = {
                "event": "get_devices_by_pod",
                "pod_id": pod_id
            }
            logging.info(f"Sending message: {to_send}")
            self.client.send_message("get_devices_by_pod", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_get_devices_by_pod_event.wait(timeout=10)

            with lock_get_devices_by_pod_response:
                response = parse_and_flatten_messages(messages_get_devices_by_pod_response)
            logging.info(f"Received data GET DEVICES BY POD: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while fetching devices by pod: {e}")
            raise

    def update_device(self, record_id, device_dto):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_update_device_response:
                messages_update_device_response.clear()
            messages_consumed_update_device_event.clear()

            update_mapper = UpdateDeviceMapper(id=record_id)
            update_mapper.update_from_dict(device_dto.model_dump())

            to_send = {
                "event": "update",
                "data": update_mapper.model_dump_not_none()
            }

            logging.info(f"Sending message: {to_send}")
            self.client.send_message("update_device", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_device_event.wait(timeout=10)

            with lock_update_device_response:
                response = parse_and_flatten_messages(messages_update_device_response)
            logging.info(f"Received data UPDATE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while updating device: {e}")
            raise

    def delete_device(self, record_id: int):
        try:
            logging.debug("Clearing previous messages and events.")
            with lock_delete_device_response:
                messages_delete_device_response.clear()
            messages_consumed_delete_device_event.clear()

            to_send = {
                "event": "delete",
                "id": record_id
            }

            logging.info(f"Sending message: {to_send}")
            self.client.send_message("delete_device", to_send)

            logging.info("Waiting for message consumption event to be set.")
            messages_consumed_device_event.wait(timeout=10)

            with lock_delete_device_response:
                response = parse_and_flatten_messages(messages_delete_device_response)
            logging.info(f"Received data DELETE: {response}")
            return response

        except Exception as e:
            logging.error(f"An error occurred while deleting device: {e}")
            raise
