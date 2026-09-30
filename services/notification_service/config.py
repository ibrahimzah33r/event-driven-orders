from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    database_url: str

    kafka_bootstrap_servers: str = (
        "127.0.0.1:9092"
    )

    notification_events_topic: str = (
        "notifications.events"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()