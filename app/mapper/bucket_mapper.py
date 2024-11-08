from pydantic import BaseModel


class BucketMapper(BaseModel):
    serial: str
    name: str
    description: str | None = None
    device_id: int
    user_id: int | None = None


class UpdateBucketMapper(BaseModel):
    id: int
    serial: str | None = None
    name: str | None = None
    description: str | None = None
    device_id: int | None = None
    user_id: int | None = None

    def update_from_dict(self, data: dict):
        for field, value in data.items():
            if hasattr(self, field) and value is not None:
                setattr(self, field, value)
        return self

    def model_dump_not_none(self):
        return {k: v for k, v in self.model_dump().items() if v is not None}

