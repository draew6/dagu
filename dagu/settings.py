from pydantic_settings import BaseSettings


class DaguSettings(BaseSettings):
    dagu_base_url: str
    dagu_username: str
    dagu_password: str
