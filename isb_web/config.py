from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg2://isb_writer:isamplesinabox@db/isb_1"
    web_root: str = "/"
    solr_url: str = "http://solr:8983/solr/isb_core_records"

    class Config:
        env_file = "isb_web_config.env"
