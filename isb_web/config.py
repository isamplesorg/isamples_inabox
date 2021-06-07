from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg2://isb_writer:1234@localhost/isb_1"
    web_root: str = "/"

    class Config:
        env_file = "isb_web_config.env"
