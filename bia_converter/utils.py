import os
import uuid
import hashlib
import logging
import tempfile

import bia_integrator_api.models as api_models

from .io import upload_dirpath_as_zarr_image_rep, stage_fileref_and_get_fpath, copy_local_to_s3
from .scli import rw_client, get_study_uuid_by_accession_id
from .rendering import generate_padded_thumbnail_from_ngff_uri
from .conversion import cached_convert_to_zarr_and_get_fpath


logger = logging.getLogger(__name__)


def generate_identifier_for_single_fileref_image(fileref):
    """Generate a UUID for an image generated from a single file reference."""

    hash_input = fileref.uuid
    hexdigest = hashlib.md5(hash_input.encode("utf-8")).hexdigest()
    image_id_as_uuid = uuid.UUID(version=4, hex=hexdigest)
    image_id = str(image_id_as_uuid)

    return image_id


def create_and_persist_image_from_fileref(study_uuid, fileref, rep_type="fire_object"):
    """Create a new image, together with a single representation from one file
    reference."""

    name = fileref.name
    image_id = generate_identifier_for_single_fileref_image(fileref)

    logger.info(f"Assigned name {name}, id {image_id}")

    image_rep = api_models.BIAImageRepresentation(
        image_id=image_id,
        size=fileref.size_in_bytes,
        uri=[fileref.uri],
        attributes={"fileref_ids": [fileref.uuid]},
        type=rep_type
    )

    image = api_models.BIAImage(
        uuid=image_id,
        version=0,
        study_uuid=study_uuid,
        original_relpath=name,
        name=name,
        representations=[image_rep],
        attributes=fileref.attributes
    )

    rw_client.create_images([image])


def get_representation_by_type(study_uuid: str, image_name: str, rep_type="ome_ngff"):

    search_filter = api_models.SearchImageFilter(
        study_uuid=study_uuid,
        original_relpath=image_name,
        image_representations_any=[api_models.SearchFileRepresentation(
            type=rep_type
        )]
    )

    images = rw_client.search_images_exact_match(search_filter)

    if len(images) > 0:
        image = images[0]
        reps_by_type = {rep.type: rep for rep in image.representations}
        return reps_by_type[rep_type]
    else:
        return None


def get_image_by_accession_id_and_name(accession_id: str, name: str):

    study_uuid = get_study_uuid_by_accession_id(accession_id)

    search_filter = api_models.SearchImageFilter(
        study_uuid=study_uuid,
        original_relpath=name
    )

    images = rw_client.search_images_exact_match(search_filter)

    if len(images) > 0:
        return images[0]
    else:
        return None


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


def create_thumbnail_by_accession_id_and_name(accession_id, image_name):
    image = get_image_by_accession_id_and_name(accession_id, image_name)
    reps_by_type = {rep.type: rep for rep in image.representations}

    rep = reps_by_type["ome_ngff"]
    ome_zarr_uri = rep.uri[0]

    dims = 256, 256
    w, h = dims
    im = generate_padded_thumbnail_from_ngff_uri(ome_zarr_uri, dims)

    dst_key = f"{accession_id}/{image.uuid}/{image.uuid}-thumbnail-{w}-{h}.png"

    with tempfile.NamedTemporaryFile(suffix=".png") as fh:
        im.save(fh)
        thumbnail_uri = copy_local_to_s3(fh.name, dst_key)
        logger.info(f"Wrote thumbnail to {thumbnail_uri}")
        size = os.stat(fh.name).st_size

    representation = api_models.BIAImageRepresentation(
        size=size,
        type="thumbnail",
        uri=[thumbnail_uri],
        dimensions=str(dims),
        rendering=None,
        attributes={}
    )

    rw_client.create_image_representation(image.uuid, representation)


def create_representative_by_accession_id_and_name(accession_id, image_name):
    image = get_image_by_accession_id_and_name(accession_id, image_name)
    reps_by_type = {rep.type: rep for rep in image.representations}

    rep = reps_by_type["ome_ngff"]
    ome_zarr_uri = rep.uri[0]

    dims = 512, 512
    w, h = dims
    im = generate_padded_thumbnail_from_ngff_uri(ome_zarr_uri, dims)

    dst_key = f"{accession_id}/{image.uuid}/{image.uuid}-representative-{w}-{h}.png"

    with tempfile.NamedTemporaryFile(suffix=".png") as fh:
        im.save(fh)
        thumbnail_uri = copy_local_to_s3(fh.name, dst_key)
        logger.info(f"Wrote representative image to {thumbnail_uri}")
        size = os.stat(fh.name).st_size

    representation = api_models.BIAImageRepresentation(
        size=size,
        type="representative",
        uri=[thumbnail_uri],
        dimensions=str(dims),
        rendering=None,
        attributes={}
    )

    rw_client.create_image_representation(image.uuid, representation)

    return representation


def ensure_unique_annotation_key_value(accession_id: str, key, value):
    study_uuid = get_study_uuid_by_accession_id(accession_id)
    study = rw_client.get_study(study_uuid)

    annotations_by_key = {
        annotation.key: annotation for annotation in study.annotations 
    }

    if key not in annotations_by_key:
        annotation = api_models.ImageAnnotation(
            # FIXME - don't hardcode username
            author_email = "matthewh@ebi.ac.uk",
            key = key,
            value = value,
            state = api_models.AnnotationState.ACTIVE
        )

        study.annotations.append(annotation)
        study.version +=1

        rw_client.update_study(study)
    else:
        current_value = annotations_by_key[key].value
        if current_value != value:
            annotation = api_models.ImageAnnotation(
                author_email = "matthewh@ebi.ac.uk",
                key = key,
                value = value,
                state = api_models.AnnotationState.ACTIVE
            )     
            annotations_by_key[key] = annotation
            
            study.annotations = list(annotations_by_key.values())
            study.version += 1

            rw_client.update_study(study)