"""BIA Proxy image classes + functionality to enable determination of
image properties."""

from typing import Optional, List

import zarr
from pydantic import BaseModel

from .omezarrmeta import ZMeta, DataSet, CoordinateTransformation


class OMEZarrImage(BaseModel):

    sizeX: int
    sizeY: int
    sizeZ: int = 1
    sizeC: int = 1
    sizeT: int = 1

    n_scales: int = 1
    xy_scaling: float = 1.0
    z_scaling: float = 1.0
    path_keys: List[str]= []
        
    PhysicalSizeX: Optional[float] = None
    PhysicalSizeY: Optional[float] = None
    PhysicalSizeZ: Optional[float] = None

    ngff_metadata: ZMeta | None = None


class BIARasterImage(BaseModel):

    sizeX: int
    sizeY: int
    sizeZ: int = 1
    sizeC: int = 1
    sizeT: int = 1
        
    PhysicalSizeX: Optional[float] = None
    PhysicalSizeY: Optional[float] = None
    PhysicalSizeZ: Optional[float] = None

    licence: Optional[str] = None
        
    imaging_type: Optional[str] = None
    organism: Optional[str] = None
    biological_entity: Optional[str] = None
    specimen_prep: Optional[str] = None
        
    study_title: Optional[str] = None
    study_description: Optional[str] = None
    study_accession_id: Optional[str] = None

    image_landing_uri: Optional[str] = None

    original_relpath: Optional[str] = None

    image_id: Optional[str] = None
    thumbnail_uri: Optional[str] = None
        
# FIXME? - should we allow no unit? Propagate unknowns?
UNIT_LOOKUP = {
    None: 1,
    "meter": 1,
    "micrometer": 1e-6,
    "nanometer": 1e-9,
    "angstrom": 1e-10,
    "femtometer": 1e-15
}

AXIS_NAME_LOOKUP = {
    "x": "PhysicalSizeX",
    "y": "PhysicalSizeY",
    "z": "PhysicalSizeZ"
}

def calculate_voxel_to_physical_factors(ngff_metadata, ignore_unit_errors=False):
    """Given ngff_metadata, calculate the voxel to physical space scale factors
    in m for each spatial dimension.
    
    NOTE: Makes a lot of assumptions about ordering of multiscales, datasets and transforms."""
    
    scale_transformations = [
        ct
        for ct in ngff_metadata.multiscales[0].datasets[0].coordinateTransformations
        if ct.type == 'scale'
    ]

    factors = {}
    
    for scale, axis in zip(scale_transformations[0].scale, ngff_metadata.multiscales[0].axes):
        if axis.type == 'space':
            attribute_name = AXIS_NAME_LOOKUP[axis.name]
            unit_multiplier = UNIT_LOOKUP.get(axis.unit, None)
            if unit_multiplier is not None:
                attribute_value = scale * unit_multiplier
                factors[attribute_name] = attribute_value
            else:
                if not ignore_unit_errors:
                    raise Exception(f"Don't know unit {axis.unit}")
            
    return factors


def ome_zarr_image_from_ome_zarr_uri(uri, ignore_unit_errors=False):
    """Generate a OME Zarr image object by reading an OME Zarr and
    parsing the NGFF metadata for properties. Makes many assumptions
    about ordering of multiscales data."""
    
    zgroup = zarr.open(uri)
    ngff_metadata = ZMeta.parse_obj(zgroup.attrs.asdict())

    path_key = ngff_metadata.multiscales[0].datasets[0].path
    zarray = zgroup[path_key]
    tdim, cdim, zdim, ydim, xdim = zarray.shape

    ome_zarr_image = OMEZarrImage(
        sizeX=xdim,
        sizeY=ydim,
        sizeZ=zdim,
        sizeC=cdim,
        sizeT=tdim,
        path_keys = [ds.path for ds in ngff_metadata.multiscales[0].datasets]
    )
    
    scale_factors = scales_from_ngff_metadata(ngff_metadata)
    ome_zarr_image.__dict__.update(scale_factors)

    factors = calculate_voxel_to_physical_factors(ngff_metadata, ignore_unit_errors)
    ome_zarr_image.__dict__.update(factors)

    ome_zarr_image.ngff_metadata = ngff_metadata
    
    return ome_zarr_image


def scales_from_ngff_metadata(ngff_metadata):
    """Derive numbers of multiscales and xy/z scaling factors from NGFF metadata.
    Assumes all scaling factors are equal."""

    n_scales = len(ngff_metadata.multiscales[0].datasets)

    scale_factors = {"n_scales": n_scales}

    if n_scales > 1:
        _, _, zscale0, yscale0, xscale0 = ngff_metadata.multiscales[0].datasets[0].coordinateTransformations[0].scale
        _, _, zscale1, yscale1, xscale1 = ngff_metadata.multiscales[0].datasets[1].coordinateTransformations[0].scale

        yscaling = yscale1 / yscale0
        xscaling = xscale1 / xscale0
        assert xscaling == yscaling, "Different X and Y scaling is not well handled"

        scale_factors["xy_scaling"] = xscaling
        scale_factors["z_scaling"] = zscale1 / zscale0

    return scale_factors


def bia_raster_image_from_ome_zarr_uri(uri):
    """Generate a BIA raster image object by reading an OME Zarr and
    parsing the NGFF metadata for properties. Makes many assumptions
    about ordering of multiscales data."""
    
    zgroup = zarr.open(uri)
    ngff_metadata = ZMeta.parse_obj(zgroup.attrs.asdict())

    path_key = ngff_metadata.multiscales[0].datasets[0].path
    zarray = zgroup[path_key]
    tdim, cdim, zdim, ydim, xdim = zarray.shape

    bia_raster_image = BIARasterImage(
        sizeX=xdim,
        sizeY=ydim,
        sizeZ=zdim,
        sizeC=cdim,
        sizeT=tdim
    )
    
    factors = calculate_voxel_to_physical_factors(ngff_metadata)
    bia_raster_image.__dict__.update(factors)
    
    return bia_raster_image


def generate_datasets(ome_zarr_image: OMEZarrImage):

    start_z = ome_zarr_image.PhysicalSizeZ
    start_y = ome_zarr_image.PhysicalSizeY
    start_x = ome_zarr_image.PhysicalSizeX
    factor_z = ome_zarr_image.z_scaling
    factor_x = ome_zarr_image.xy_scaling
    factor_y = ome_zarr_image.xy_scaling


    datasets = [
        DataSet(
            path=path_label,
            coordinateTransformations=[
                CoordinateTransformation(
                    scale=[1., 1., start_z * factor_z ** n, start_y * factor_y ** n, start_x * factor_x ** n],
                    type="scale"
                )
            ]
        )
        for n, path_label in enumerate(ome_zarr_image.path_keys)
    ]

    return datasets