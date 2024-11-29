from app.shared import *
from RSKafkaWrapper.client import KafkaClient
from app.config.config import Settings

app_settings = Settings()
kafka_client = KafkaClient.instance(app_settings.kafka_bootstrap_servers, app_settings.kafka_group_id)
