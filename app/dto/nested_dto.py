from typing import List
from app.dto.pod_dto import PodDTO
from app.dto.device_dto import DeviceDTO
from app.dto.bucket_dto import BucketDTO


class PodWithDevicesDTO(PodDTO):
    devices: List[DeviceDTO] = []


class DeviceWithBucketsDTO(DeviceDTO):
    buckets: List[BucketDTO] = []
