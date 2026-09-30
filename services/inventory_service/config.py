from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    database_url: str

    kafka_bootstrap_servers: str = (
        "127.0.0.1:9092"
    )

    order_events_topic: str = "orders.events"
    inventory_events_topic: str = "inventory.events"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()