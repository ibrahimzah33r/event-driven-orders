from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    database_url: str

    kafka_bootstrap_servers: str = (
        "localhost:9092"
    )

    order_events_topic: str = "orders.events"
    payment_events_topic: str = "payments.events"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()