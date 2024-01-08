from typing import Dict, List
from pathlib import Path

from ruamel.yaml import YAML
from pydantic import BaseSettings, BaseModel


class Settings(BaseSettings):
    endpoint_url: str = "https://uk1s3.embassy.ebi.ac.uk"
    bucket_name: str = "bia-integrator-data"
    cache_root_dirpath: Path = Path.home()/".cache"/"bia-converter"
    bioformats2raw_java_home: str
    bioformats2raw_bin: str
    config_fpath: Path = Path("bia.yaml")

    class Config:
        env_file = '.env'

settings = Settings()


class ConversionSettings(BaseModel):
    convert_all: bool = False


class ConversionOptions(BaseModel):
    transpose_t_z: bool = False


class ImageToConvert(BaseModel):
    name: str
    options: ConversionOptions = ConversionOptions()


class StudySettings(BaseModel):
    representative_image: ImageToConvert | None
    images_to_convert: List[ImageToConvert] = []
    conversion_settings: ConversionSettings = ConversionSettings()


class ConversionConfig(BaseModel):
    studies: Dict[str, StudySettings]


def load_raw_config():
    yaml = YAML()
    with open(settings.config_fpath) as fh:
        raw_config = yaml.load(fh)

    return raw_config


def load_config():
    raw_config = load_raw_config()

    return ConversionConfig.parse_obj(raw_config)
