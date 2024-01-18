import uuid
import hashlib
from pathlib import Path

import rich
import parse
from bia_integrator_api.models import BIAImage, BIAImageRepresentation


from .models import StructuredFileset
from .scli import (
    rw_client,
    get_image_by_name,
    get_study_uuid_by_accession_id
)


def identifier_from_fileref_ids(fileref_ids):
    hash_input = ''.join(fileref_ids)
    hexdigest = hashlib.md5(hash_input.encode("utf-8")).hexdigest()
    image_id_as_uuid = uuid.UUID(version=4, hex=hexdigest)
    image_id = str(image_id_as_uuid)

    return image_id


def fileref_map_to_pattern(fileref_map, ext):
    """Determine the pattern needed to enable bioformats to load the components
    of a structured fileset, e.g. for conversion."""

    all_positions = list(fileref_map.values())
    tvals, cvals, zvals = zip(*all_positions)

    zmin = min(zvals)
    zmax = max(zvals)
    cmin = min(cvals)
    cmax = max(cvals)
    tmin = min(tvals)
    tmax = max(tvals)

    pattern = f"T<{tmin:04d}-{tmax:04d}>_C<{cmin:04d}-{cmax:04d}>_Z<{zmin:04d}-{zmax:04d}>{ext}"

    return pattern


def assign_image_idem(accession_id: str, image_to_convert):

    study_uuid = get_study_uuid_by_accession_id(accession_id)
    image = get_image_by_name(study_uuid, image_to_convert.name)

    if not image:
        assign_image_from_image_to_convert(accession_id, image_to_convert)


def assign_image_from_image_to_convert(accession_id, image_to_convert):

    study_uuid = get_study_uuid_by_accession_id(accession_id)

    file_references = rw_client.get_study_file_references(study_uuid, limit=10000)

    parse_template = image_to_convert.pattern
    fileref_map = {}
    selected_filerefs = []
    for fileref in file_references:
        result = parse.parse(parse_template, fileref.name)
        if result:
            z = result.named.get('z', 0)
            c = result.named.get('c', 0)
            t = result.named.get('t', 0)
            fileref_map[fileref.uuid] = (t, c, z)
            selected_filerefs.append(fileref)

    extensions = {Path(fileref.name).suffix for fileref in selected_filerefs}
    assert len(extensions) == 1
    common_ext = list(extensions)[0]

    sf = StructuredFileset(
        fileref_map=fileref_map,
        attributes={
            'extension': common_ext
        }
    )

    fileref_ids = [fileref.uuid for fileref in selected_filerefs]
    image_id = identifier_from_fileref_ids(fileref_ids)

    image_rep = BIAImageRepresentation(
        accession_id=accession_id,
        image_id=image_id,
        size=sum(fileref.size_in_bytes for fileref in selected_filerefs),
        uri=[fileref.uri for fileref in selected_filerefs],
        attributes={
            "structured_fileset": sf.dict()
        },
        type="structured_fileset",
        dimensions=None,
        rendering=None
    )

    image = BIAImage(
        uuid=image_id,
        study_uuid=study_uuid,
        accession_id=accession_id,
        original_relpath=image_to_convert.name,
        name=image_to_convert.name,
        representations=[image_rep],
        version=0
    )

    rw_client.create_images([image])