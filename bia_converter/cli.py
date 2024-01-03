import logging

import rich
import typer

from bia_converter.config import load_config, StudySettings
from bia_converter.scli import rw_client, get_study_uuid_by_accession_id
from bia_converter.utils import (
    create_and_persist_image_from_fileref,
    get_representation_by_type,
    convert_by_accession_id_and_name,
    create_thumbnail_by_accession_id_and_name,
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
    rich.print(work_plan)

    funcs = [
        create_thumbnail_by_accession_id_and_name,
        convert_by_accession_id_and_name
    ]

    func_lookup = { func.__name__: func for func in funcs }

    for func, accession_id, name in work_plan:
        func_lookup[func](accession_id, name)
        


@app.command()
def convert(accession_id: str):

    logging.basicConfig(level=logging.INFO)
    raw_config = load_config()
    study_uuid = get_study_uuid_by_accession_id(accession_id)

    study_settings = StudySettings.parse_obj(raw_config["studies"][accession_id])

    work_plan = []
    if study_settings.conversion_settings.convert_all:
        all_images = rw_client.get_study_images(study_uuid, limit=10**6)
        for image in all_images:
            if not get_representation_by_type(study_uuid, image.name, rep_type="ome_ngff"):
                work_plan.append(("convert_by_accession_id_and_name", accession_id, image.name))
            if not get_representation_by_type(study_uuid, image.name, rep_type="thumbnail"):
                work_plan.append(("create_thumbnail_by_accession_id_and_name", accession_id, image.name))
    else:
        images_to_check = raw_config["studies"][accession_id]["images_to_convert"]

        for image in images_to_check:
            if not get_representation_by_type(study_uuid, image["name"], rep_type="ome_ngff"):
                ensure_assigned(study_uuid, image["name"])
                work_plan.append(("convert_by_accession_id_and_name", accession_id, image["name"]))
            if not get_representation_by_type(study_uuid, image["name"], rep_type="thumbnail"):
                work_plan.append(("create_thumbnail_by_accession_id_and_name", accession_id, image["name"]))

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

    fileref_name = raw_config["studies"][accession_id]["to_assign"][0]['name']

    study_uuid = get_study_uuid_by_accession_id(accession_id)
    for fileref in raw_config["studies"][accession_id]["to_assign"]:
        ensure_assigned(study_uuid, fileref["name"])


@app.command()
def set_representative(accession_id: str):
    logging.basicConfig(level=logging.INFO)

    study_uuid = get_study_uuid_by_accession_id(accession_id)

    raw_config = load_config()
    image_name = raw_config["studies"][accession_id]["representative_image"]['name']

    rep = ensure_representative_rep_exists(accession_id, image_name)
    ensure_unique_annotation_key_value(accession_id, "example_image_uri", rep.uri[0])


if __name__ == "__main__":
    app()