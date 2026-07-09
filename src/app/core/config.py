from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "events"
    es_url: str = "http://localhost:9200"
    es_index: str = "events"
    es_field_limit: int = 200
    redis_url: str = "redis://localhost:6379/0"

    queue_max_depth: int = 10_000
    retry_base_delay: float = 1.0
    max_receive_count: int = 5
    worker_batch_size: int = 10
    worker_concurrency: int = 1

    realtime_cache_ttl: int = 30
    realtime_window_seconds: int = 300

    query_default_limit: int = 50
    query_max_limit: int = 500
    query_max_offset: int = 10_000
    search_max_size: int = 100

    metadata_max_bytes: int = 16_384
    timestamp_max_future_skew_seconds: int = 300

    rate_limit_window_seconds: int = 60
    rate_limit_writes_per_window: int = 120
    rate_limit_reads_per_window: int = 600

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
