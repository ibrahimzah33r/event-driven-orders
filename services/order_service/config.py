from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    kafka_bootstrap_servers: str

    payment_events_topic: str = "payments.events"
    inventory_events_topic: str = "inventory.events"
    notification_events_topic: str = "notifications.events"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()