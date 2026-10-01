from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql+psycopg://payments:payments@localhost:5432/payments"
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic: str = "payments.events.v1"
    database_connect_timeout: int = 2
    worker_recovery_backoff: float = 0.5

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            os.getenv("DATABASE_URL", cls.database_url),
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            int(os.getenv("DATABASE_CONNECT_TIMEOUT", str(cls.database_connect_timeout))),
            float(os.getenv("WORKER_RECOVERY_BACKOFF", str(cls.worker_recovery_backoff))),
        )
