import logging
import tempfile
from pathlib import Path

import rich
from bia_integrator_api.models import ( # type: ignore
    ImageRepresentation, ImageRepresentationUseType
)
from bia_shared_datamodels.uuid_creation import ( # type: ignore
    create_image_representation_uuid
)

from .io import copy_local_to_s3
from .bia_api_client import api_client, store_object_in_api_idempotent
from .rendering import generate_padded_thumbnail_from_ngff_uri
from .utils import create_s3_uri_suffix_for_image_representation


logger = logging.getLogger(__file__)


def create_image_representation_object(image, image_format, use_type):
    # Create the base image representation object. Cannot by itself create the file_uri or
    # size attributes correctly

    image_rep_uuid = create_image_representation_uuid(image, image_format, use_type)
    image_rep = ImageRepresentation(
        uuid=str(image_rep_uuid),
        version=0,
        representation_of_uuid=image.uuid,
        use_type=use_type,
        image_format=image_format,
        attribute=[],
        total_size_in_bytes=0,
        file_uri=[],

    )

    return image_rep


def create_2d_image_and_upload_to_s3(ome_zarr_uri, dims, dst_key):
    im = generate_padded_thumbnail_from_ngff_uri(ome_zarr_uri, dims)

    with tempfile.NamedTemporaryFile(suffix=".png") as fh:
        im.save(fh)
        file_uri = copy_local_to_s3(Path(fh.name), dst_key)
        logger.info(f"Wrote thumbnail to {file_uri}")
        size_in_bytes = Path(fh.name).stat().st_size

    return file_uri, size_in_bytes


def convert_interactive_display_to_thumbnail(input_imagerep_uuid: str, dims=(256, 256)):
    # Should convert an INTERACTIVE_DISPLAY rep, to a THUMBNAIL rep

    # Retrieve and check the image rep
    input_image_rep = api_client.get_image_representation(input_imagerep_uuid)
    assert input_image_rep.use_type == ImageRepresentationUseType.INTERACTIVE_DISPLAY

    # Retrieve model ibjects
    input_image = api_client.get_image(input_image_rep.representation_of_uuid)
    dataset = api_client.get_dataset(input_image.submission_dataset_uuid)
    study = api_client.get_study(dataset.submitted_in_study_uuid)

    base_image_rep = create_image_representation_object(input_image, ".png", "THUMBNAIL")
    w, h = dims
    base_image_rep.size_x = w
    base_image_rep.size_y = h

    dst_key = create_s3_uri_suffix_for_image_representation(study.accession_id, base_image_rep)
    file_uri, size_in_bytes = create_2d_image_and_upload_to_s3(input_image_rep.file_uri[0], dims, dst_key)

    base_image_rep.file_uri = [file_uri]
    base_image_rep.total_size_in_bytes = size_in_bytes

    store_object_in_api_idempotent(base_image_rep)

    return base_image_rep


def convert_interactive_display_to_static_display(input_imagerep_uuid: str, dims=(512, 512)):
    # Should convert an INTERACTIVE_DISPLAY rep, to a STATIC_DISPLAY rep

    # Retrieve and check the image rep
    input_image_rep = api_client.get_image_representation(input_imagerep_uuid)
    assert input_image_rep.use_type == ImageRepresentationUseType.INTERACTIVE_DISPLAY

    # Retrieve model ibjects
    input_image = api_client.get_image(input_image_rep.representation_of_uuid)
    dataset = api_client.get_dataset(input_image.submission_dataset_uuid)
    study = api_client.get_study(dataset.submitted_in_study_uuid)

    base_image_rep = create_image_representation_object(input_image, ".png", "STATIC_DISPLAY")
    w, h = dims
    base_image_rep.size_x = w
    base_image_rep.size_y = h

    dst_key = create_s3_uri_suffix_for_image_representation(study.accession_id, base_image_rep)
    file_uri, size_in_bytes = create_2d_image_and_upload_to_s3(input_image_rep.file_uri[0], dims, dst_key)

    base_image_rep.file_uri = [file_uri]
    base_image_rep.total_size_in_bytes = size_in_bytes

    store_object_in_api_idempotent(base_image_rep)

    return base_image_rep