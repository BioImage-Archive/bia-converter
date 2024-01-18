import logging

import rich
import parse
import typer

from .assign import assign_image_idem
from bia_converter.config import load_config, StudySettings
from bia_converter.scli import rw_client, get_study_uuid_by_accession_id, get_image_by_name
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









import tempfile
from pathlib import Path

from bia_integrator_api.models import BIAImageRepresentation

from .config import settings
from .io import stage_fileref_and_get_fpath, upload_dirpath_as_zarr_image_rep
from .conversion import run_zarr_conversion
from .utils import check_for_uploaded_s3_zarr

@app.command()
def spike_multiplane_convert(accession_id: str):
    logging.basicConfig(level=logging.INFO)

    conversion_config = load_config()
    images_to_convert = conversion_config.studies[accession_id].images_to_convert
    image_to_convert = images_to_convert[0]

    study_uuid = get_study_uuid_by_accession_id(accession_id)
    image = get_image_by_name(study_uuid=study_uuid, image_name=image_to_convert.label)
    
    image_rep = image.representations[0]
    # print(rep.attributes)
    sf = StructuredFileset.parse_obj(image_rep.attributes['structured_fileset'])

    tmpdir_obj = tempfile.TemporaryDirectory()
    tmpdir_path = Path(tmpdir_obj.name)

    file_references = rw_client.get_study_file_references(study_uuid, limit=10000)
    file_references_by_uuid = {fileref.uuid: fileref for fileref in file_references}
    for fileref_id, position in sf.fileref_map.items():
        # print(z, fileref_id)
        t, c, z = position
        label = "T{t:04d}_C{c:04d}_Z{z:04d}".format(z=z, c=c, t=t)

        fileref = file_references_by_uuid[fileref_id]
        input_fpath = stage_fileref_and_get_fpath(fileref)

        suffix = input_fpath.suffix

        target_path = tmpdir_path/(label+suffix)
        logger.info(f"Linking {input_fpath} as {target_path}")

        target_path.symlink_to(input_fpath)

    # FIXME - we should be deriving the extension
    # pattern = fileref_map_to_pattern(sf.fileref_map, ".dm3")
    pattern = fileref_map_to_pattern(sf.fileref_map, ".tiff")

    pattern_fpath = tmpdir_path / "conversion.pattern"
    logger.info(f"Using pattern {pattern}")
    pattern_fpath.write_text(pattern)

    dst_dir_basepath = settings.cache_root_dirpath/"zarr"
    dst_dir_basepath.mkdir(exist_ok=True, parents=True)
    zarr_fpath = dst_dir_basepath/f"{image.uuid}.zarr"
    logger.info(f"Destination fpath: {zarr_fpath}")
    if not zarr_fpath.exists():
        run_zarr_conversion(pattern_fpath, zarr_fpath)

    zarr_to_upload = zarr_fpath
    # upload_dirpath_as_zarr_image_rep(zarr_to_upload, accession_id, image.uuid)
    zarr_rep_uri = check_for_uploaded_s3_zarr(accession_id, image)
    rich.print(f"Current zarr uri: {zarr_rep_uri}")

    path_in_zarr = '/0'
    all_representation_types = { rep.type for rep in image.representations }
    if 'ome_ngff' not in all_representation_types:
        logger.info(f"Creating ome_ngff representation")
        representation = BIAImageRepresentation(
            size=0,
            type="ome_ngff",
            uri=[zarr_rep_uri + path_in_zarr],
            dimensions=None,
            rendering=None,
            attributes={}
        )

        rw_client.create_image_representation(image.uuid, representation)


@app.command()
def spike_assign(accession_id: str):
    conversion_config = load_config()

    images_to_convert = conversion_config.studies[accession_id].images_to_convert

    for image_to_convert in images_to_convert:
        assign_image_idem(accession_id, image_to_convert)


from .utils import cached_convert_from_structured_fileset
@app.command()
def spike2_convert(accession_id: str):
    logging.basicConfig(level=logging.INFO)
    conversion_config = load_config()
    images_to_convert = conversion_config.studies[accession_id].images_to_convert
    image_to_convert = images_to_convert[0]  

    cached_convert_from_structured_fileset(accession_id, image_to_convert)  


from .omezarrmeta import Axis, MultiScaleImage
from .proxyimage import ome_zarr_image_from_ome_zarr_uri, generate_datasets
@app.command()
def spike_fix_scales(fpath: str):
    ome_zarr_image = ome_zarr_image_from_ome_zarr_uri(fpath)

    rich.print(ome_zarr_image)

    ome_zarr_image.PhysicalSizeZ = 2.8e-5

    default_axes = [
        Axis(name='t', type='time', unit=None),
        Axis(name='c', type='channel', unit=None),
        Axis(name='z', type='space', unit='meter'),
        Axis(name='x', type='space', unit='meter'),
        Axis(name='y', type='space', unit='meter')
    ]

    ngff_metadata = ome_zarr_image.ngff_metadata
    ngff_metadata.multiscales = [MultiScaleImage(
        datasets=generate_datasets(ome_zarr_image),
        metadata=ngff_metadata.multiscales[0].metadata,
        axes=default_axes,
        version='0.4'
    )]

    new_ngff_metadata_str = ngff_metadata.json(indent=2, exclude_none=True)
    rich.print(ngff_metadata)

    with open("checkme.zattrs", "w") as fh:
        fh.write(new_ngff_metadata_str)



    

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