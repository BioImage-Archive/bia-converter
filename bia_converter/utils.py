from uuid import UUID

from bia_integrator_api.models import ( # type: ignore
    ImageRepresentation,
)

def create_s3_uri_suffix_for_image_representation(
    accession_id: str, representation: ImageRepresentation
) -> str:
    """Create the part of the s3 uri that goes after the bucket name for an image representation"""

    assert representation.image_format and len(representation.image_format) > 0
    assert isinstance(representation.representation_of_uuid, UUID) or isinstance(
        UUID(representation.representation_of_uuid), UUID
    )
    return f"{accession_id}/{representation.representation_of_uuid}/{representation.uuid}{representation.image_format}"

