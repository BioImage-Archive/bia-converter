from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):
    endpoint_url: str = "https://uk1s3.embassy.ebi.ac.uk"
    bucket_name: str = "bia-integrator-data"
    cache_root_dirpath: Path = Path.home()/".cache"/"bia-converter"
    bioformats2raw_java_home: str
    bioformats2raw_bin: str

    class Config:
        env_file = '.env'



settings = Settings()