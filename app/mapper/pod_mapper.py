from pydantic import BaseModel


class PodMapper(BaseModel):
    name: str
    description: str | None = None
    user_id: int | None = None


class UpdatePodMapper(BaseModel):
    id: int
    name: str | None = None
    description: str | None = None
    user_id: int | None = None

    def update_from_dict(self, data: dict):
        for field, value in data.items():
            if hasattr(self, field) and value is not None:
                setattr(self, field, value)
        return self

    def model_dump_not_none(self):
        return {k: v for k, v in self.model_dump().items() if v is not None}
