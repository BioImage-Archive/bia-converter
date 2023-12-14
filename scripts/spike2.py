import logging
from pathlib import Path

import typer
from ruamel.yaml import YAML
from bia_integrator_api import models as api_models


from bia_converter.io import stage_fileref_and_get_fpath, upload_dirpath_as_zarr_image_rep
from bia_converter.scli import rw_client, get_study_uuid_by_accession_id
from bia_converter.conversion import cached_convert_to_zarr_and_get_fpath


logger = logging.getLogger(__file__)


app = typer.Typer()


def check_if_rep_exists(accession_id: str, image_name: str, rep_type="ome_ngff"):

    study_uuid = get_study_uuid_by_accession_id(accession_id)

    search_filter = api_models.SearchImageFilter(
        study_uuid=study_uuid,
        original_relpath=image_name,
        image_representations_any=[api_models.SearchFileRepresentation(
            type="ome_ngff"
        )]
    )

    images = rw_client.search_images_exact_match(search_filter)

    return len(images) > 0


def get_image_by_accession_id_and_name(accession_id: str, name: str):

    study_uuid = get_study_uuid_by_accession_id(accession_id)

    search_filter = api_models.SearchImageFilter(
        study_uuid=study_uuid,
        original_relpath=name
    )

    images = rw_client.search_images_exact_match(search_filter)

    assert len(images) == 1

    return images[0]


def convert_by_accession_id_and_name(accession_id, image_name):

    image = get_image_by_accession_id_and_name(accession_id, image_name)
    reps_by_type = {rep.type: rep for rep in image.representations}

    rep = reps_by_type["fire_object"]
    fileref_id = rep.attributes["fileref_ids"][0]
    fileref = rw_client.get_file_reference(fileref_id)

    input_fpath = stage_fileref_and_get_fpath(fileref)
    zarr_fpath = cached_convert_to_zarr_and_get_fpath(image, input_fpath)

    zarr_rep_uri = upload_dirpath_as_zarr_image_rep(zarr_fpath, accession_id, image.uuid)

    representation = api_models.BIAImageRepresentation(
        size=0,
        type="ome_ngff",
        uri=[zarr_rep_uri + "/0"],
        dimensions=None,
        rendering=None,
        attributes={}
    )

    rw_client.create_image_representation(image.uuid, representation)


@app.command()
def generate_plan(config_fpath: Path = Path("dome.yaml")):

    logging.basicConfig(level=logging.INFO)

    yaml = YAML()
    with open(config_fpath) as fh:
        raw_config = yaml.load(fh)

    accession_id = "S-BIAD582"

    images_to_check = raw_config["studies"][accession_id]["images_to_convert"]

    work_plan = []
    for image in images_to_check:
        if not check_if_rep_exists(accession_id, image["name"]):
            work_plan.append(("convert_by_accession_id_and_name", accession_id, image["name"]))

    for func, accession_id, image_name in work_plan:
        convert_by_accession_id_and_name(accession_id, image_name)





if __name__ == "__main__":
    app()