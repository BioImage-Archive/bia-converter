import logging

import rich
import typer

from bia_converter.config import load_config, StudySettings
from bia_converter.scli import rw_client, get_study_uuid_by_accession_id
from bia_converter.utils import (
    create_and_persist_image_from_fileref,
    get_representation_by_type,
    convert_by_accession_id_and_image_descriptor,
    create_thumbnail_by_accession_id_and_image_descriptor,
    create_representative_by_accession_id_and_name,
    ensure_unique_annotation_key_value
)


app = typer.Typer()

logger = logging.getLogger(__name__)


def search_file_references_by_name(study_uuid, name):

    # FIXME - this iteration is ugly, and the limit is a problem
    file_references = rw_client.get_study_file_references(study_uuid, limit=10000)

    for fileref in file_references:
        if fileref.name == name:
            return fileref


def get_image_by_name(study_uuid, image_name):

    import bia_integrator_api.models as api_models

    search_filter = api_models.SearchImageFilter(
        study_uuid=study_uuid,
        original_relpath=image_name
    )

    images = rw_client.search_images_exact_match(search_filter)

    if len(images) > 0:
        return images[0]
    else:
        return None


def ensure_assigned(study_uuid: str, fileref_name: str):
    if not get_image_by_name(study_uuid, fileref_name):
        logger.info(f"Cannot find image with name {fileref_name}, assigning")

        fileref = search_file_references_by_name(study_uuid, fileref_name)
        create_and_persist_image_from_fileref(study_uuid, fileref)


def execute_work_plan(work_plan):
    """Given a list of tuples of the form:
    
    (function, study accession ID, image descriptor)
    
    Run the function on each image descriptor
    """

    funcs = [
        create_thumbnail_by_accession_id_and_image_descriptor,
        convert_by_accession_id_and_image_descriptor
    ]

    func_lookup = { func.__name__: func for func in funcs }

    for func, accession_id, name in work_plan:
        func_lookup[func](accession_id, name)
        


@app.command()
def convert(accession_id: str):

    logging.basicConfig(level=logging.INFO)

    conversion_config = load_config()
    study_settings = conversion_config.studies[accession_id]

    study_uuid = get_study_uuid_by_accession_id(accession_id)

    if study_settings.conversion_settings.convert_all:
        images_to_check = rw_client.get_study_images(study_uuid, limit=10**6)
    else:
        images_to_check = study_settings.images_to_convert

    work_plan = []

    # FIXME - conv targets
    for image in images_to_check:
        if not get_representation_by_type(study_uuid, image.name, rep_type="ome_ngff"):
            ensure_assigned(study_uuid, image.name)
            work_plan.append(("convert_by_accession_id_and_image_descriptor", accession_id, image))
        if not get_representation_by_type(study_uuid, image.name, rep_type="thumbnail"):
            work_plan.append(("create_thumbnail_by_accession_id_and_image_descriptor", accession_id, image))

    rich.print(work_plan)
    execute_work_plan(work_plan)

    
def ensure_representative_rep_exists(accession_id: str, image_name: str):
    study_uuid = get_study_uuid_by_accession_id(accession_id)
    rep = get_representation_by_type(study_uuid, image_name, rep_type="representative")
    if not rep:
        rep = create_representative_by_accession_id_and_name(accession_id, image_name)
    
    return rep


@app.command()
def assign(accession_id: str):

    logging.basicConfig(level=logging.INFO)

    raw_config = load_config()

    study_uuid = get_study_uuid_by_accession_id(accession_id)
    for fileref in raw_config["studies"][accession_id]["to_assign"]:
        ensure_assigned(study_uuid, fileref["name"])


@app.command()
def set_representative(accession_id: str):
    logging.basicConfig(level=logging.INFO)

    conversion_config = load_config()
    image_name = conversion_config.studies[accession_id].representative_image.name

    rep = ensure_representative_rep_exists(accession_id, image_name)
    ensure_unique_annotation_key_value(accession_id, "example_image_uri", rep.uri[0])


#FIXME
def clean():
    # aws --endpoint https://uk1s3.embassy.ebi.ac.uk s3 rm s3://bia-integrator-data/S-BIAD606/1538da69-c145-4326-9a18-496c766af86e/1538da69-c145-4326-9a18-496c766af86e.zarr --recursive
    pass


@app.command()
def check_config(accession_id: str):
    raw_config = load_config()

    print(raw_config["studies"][accession_id])


def get_easily_convertable_exts():
    from importlib import resources
    from . import data
    formats_fname = "bioformats_curated_single_file_formats.txt"
    formats_list_fpath = resources.files(data) / formats_fname
    easily_convertable_exts = { l for l in formats_list_fpath.read_text().split("\n") if len(l) > 0 }

    return easily_convertable_exts


@app.command()
def propose(accession_id: str):
    limit = 10 ** 4
    study_obj_info = rw_client.get_object_info_by_accession([accession_id]).pop()
    filerefs = rw_client.get_study_file_references(study_obj_info.uuid, limit=limit)

    from pathlib import Path

    n = 3
    exclude_exts = { '.raw' }

    exts = get_easily_convertable_exts() - exclude_exts

    eligible = [
        fileref
        for fileref in filerefs
        if Path(fileref.name).suffix in exts 
    ]
    # eligible.sort(key= lambda f: f.uuid)
    eligible.sort(key=lambda f: f.size_in_bytes, reverse=True)
    # rich.print(eligible)
    selected = eligible[:n]

    from .config import ImageToConvert, StudySettings

    images_to_convert = [
        ImageToConvert(name=s.name)
        for s in selected
    ]
    st = StudySettings(
        images_to_convert=images_to_convert,
        representative_image=images_to_convert[0]
    )
    tld = {accession_id: st.dict()}

    import sys
    from ruamel.yaml import YAML
    yaml = YAML()
    yaml.dump(tld, sys.stdout)




if __name__ == "__main__":
    app()