"""Functions to support converting Zarr arrays and turning them into OME-Zarr.

This includes:

* Rechunking of the highest resolution / largest array
* Generating downsampled representations (creating the resolution pyramid)
* Creating the OME-Zarr metadata
"""

import itertools
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import rich
import zarr
import tensorstore as ts # type: ignore

from .omezarrmeta import (
    Axis, ZMeta, DataSet, CoordinateTransformation, MSMetadata,
    MultiScaleImage,
    Omero, Channel, Window, RDefs
)


def create_omero_metadata_object(zarr_group_uri: str):
    group = zarr.open_group(zarr_group_uri)
    array_keys = list(group.array_keys())

    smallest_array = group[array_keys[-1]]
    min_val = smallest_array[:].min() # type: ignore
    max_val = smallest_array[:].max() # type: ignore

    largest_array = group[array_keys[0]]
    tdim, _, zdim, _, _ = largest_array.shape
    # rich.print(min_val, max_val, tdim, zdim)

    window = Window(
        min=0.0,
        max=255.0,
        start=min_val,
        end=max_val
    )
    channel0 = Channel(
        color="FFFFFF",
        coefficient=1,
        active=True,
        label="Channel 0",
        window=window,
        family="linear",
        inverted=False,

    )
    rdefs = RDefs(
        model="greyscale",
        defaultT=tdim//2, # type: ignore
        defaultZ=zdim//2 # type: ignore
    )
    omero = Omero(
        rdefs=rdefs,
        channels=[channel0]
    )

    return omero

def create_ome_zarr_metadata(zarr_group_uri: str, name: str, coordinate_scales: List[float]) -> ZMeta:
    """Read a Zarr group and generate the OME-Zarr metadata for that group,
    effectively turning a group of Zarr arrays into an OME-Zarr."""

    # Open the group and find the arrays in it
    group = zarr.open_group(zarr_group_uri)
    array_keys = list(group.array_keys())

    # From these arrays, get their dimensions and use these to calculate the scaling factors
    # between them
    array_dims = get_array_dims(group)
    dim_ratios = get_dimension_ratios(array_dims)

    # Use these scaling factors together with base coordinate scales to generate DataSet objects
    datasets = generate_dataset_objects(coordinate_scales, dim_ratios, array_keys)

    multiscales = generate_multiscales(datasets, name)

    omero = create_omero_metadata_object(str(zarr_group_uri))

    ome_zarr_metadata = ZMeta(
        multiscales=[multiscales],
        omero=omero
    )

    return ome_zarr_metadata


def rechunk_and_save_array(input_array_uri: str, output_dirpath: Path, target_chunks: List[int]):

    input_array_uri = ensure_uri(input_array_uri)

    source = ts.open({
        'driver': 'zarr',
        'kvstore': input_array_uri,
    }).result()

    output_spec = {
        'driver': 'zarr',
        'kvstore': {
            'driver': 'file',
            'path': str(output_dirpath)
        },
        'dtype': source.dtype.name,
        'metadata': {
            'shape': source.shape,
            'chunks': target_chunks,
            'dimension_separator': '/',
        },
    }

    output_array = ts.open(output_spec, create=True, open=True).result()
    # output_array.write(source).result()

    processing_chunk_size = [1, 1, 128, 128, 128]
    
    # Calculate number of chunks needed in each dimension
    num_chunks = tuple(
        (shape + chunk - 1) // chunk 
        for shape, chunk in zip(source.shape, processing_chunk_size)
    )
    
    # Process array in chunks
    for idx in itertools.product(*[range(n) for n in num_chunks]):
        # Calculate slice for this chunk
        slices = tuple(
            slice(i * c, min((i + 1) * c, s))
            for i, c, s in zip(idx, processing_chunk_size, source.shape)
        )
        
        # Read and write this chunk
        chunk_data = source[slices].read().result()
        output_array[slices].write(chunk_data).result()
        
        # Optional progress indication
        rich.print(f"Processed chunk {idx} of {tuple(n-1 for n in num_chunks)}")
    

def ensure_uri(path_or_uri):
    """Convert a path or string to a proper URI if needed."""
    if isinstance(path_or_uri, (str, Path)):
        parsed = urlparse(str(path_or_uri))
        if not parsed.scheme:
            # Convert local path to file:// URI
            return f"file://{Path(path_or_uri).absolute()}"
    return path_or_uri


def downsample_array_and_write_to_dirpath(
        array_uri: str,
        output_dirpath: Path,
        downsample_factors: List[int],
        output_chunks: List[int],
        downsample_method='mean'
    ):
    """
    Downsample a zarr array and save the result to a new location with specified chunking.

    This function opens a source array, downsamples it using the specified method, and writes
    the result to a new zarr array with specified chunk sizes. The dimension separator
    in the output is set to '/'.

    Args:
        array_uri: URI or path to source zarr array. If a local path is provided,
            it will be converted to a file:// URI.
        output_dirpath: Path where the downsampled array will be written.
        downsample_factors: List of integers specifying the downsample factor for each dimension.
            For example, [2, 2] will reduce the size of a 2D array by half in each dimension.
        output_chunks: List of integers specifying the chunk size for each dimension
            of the output array.
        downsample_method: string description of the downsampling method, must be one of those
            supported by tensorstore's downsampling driver

    Returns:
        None

    Example:
        >>> downsample_array_and_write_to_dirpath(
        ...     "data.zarr",
        ...     Path("downsampled.zarr"),
        ...     downsample_factors=[2, 2],
        ...     output_chunks=[256, 256]
        ... )
    """

    source = ts.open({
        'driver': 'downsample',
        'downsample_factors': downsample_factors,
        "downsample_method": downsample_method,
        'base': {
            'driver': 'zarr',
            'kvstore': {
                'driver': 'file',
                'path': array_uri
            }
        }
    }).result()

    output_spec = {
        'driver': 'zarr',
        'kvstore': {
            'driver': 'file',
            'path': str(output_dirpath) 
        },
        'dtype': source.dtype.name,
        'metadata': {
            'shape': source.shape,
            'chunks': output_chunks,
            'dimension_separator': '/'
        }
    }

    store = ts.open(output_spec, create=True, delete_existing=True).result()
    store.write(source).result()


def get_array_dims(group):
    """
    Get dimensions of all arrays in a Zarr group in order of array keys.
    
    Args:
        group: A Zarr group object
    
    Returns:
        list: Dimensions of each array in order of array keys
    """
    dims = []
    for key in sorted(group.array_keys()):
        array = group[key]
        dims.append(array.shape)
    return dims


def get_dimension_ratios(dims_list):
    """
    Process list of array dimensions, verifying rank consistency and sorting by last dimension.
    Returns list starting with a tuple of 1s, followed by ratios relative to largest array.
    
    Args:
        dims_list: List of tuples containing array dimensions
    
    Returns:
        list: List starting with ones, followed by elementwise ratios
        
    Raises:
        ValueError: If tuples have different lengths (different array ranks)
    """
    # Check if list is empty
    if not dims_list:
        return []
    
    # Verify all tuples have same length
    rank = len(dims_list[0])
    if not all(len(dims) == rank for dims in dims_list):
        raise ValueError("All arrays must have the same rank")
    
    # Sort by last dimension in descending order
    sorted_dims = sorted(dims_list, key=lambda x: x[-1], reverse=True)
    
    # Start with tuple of ones
    ratios = [tuple(1.0 for _ in range(rank))]
    
    # Calculate ratios relative to first array
    reference = sorted_dims[0]
    for dims in sorted_dims[1:]:
        ratio = tuple(d2/d1 for d1, d2 in zip(reference, dims))
        ratios.append(ratio)
    
    return ratios


def generate_axes():
    """Generate the Axis objects we use as standard for all of our OME-Zarr images."""

    initial_axes = [
        Axis(
            name="t",
            type="time",
        ),
        Axis(
            name="c",
            type="channel",
        )
    ]
    spatial_axes = [
        Axis(
            name=name,
            type="space",
            unit='meter'
        )
        for name in ["z", "y", "x"]
    ]

    return initial_axes + spatial_axes


def generate_multiscales(datasets, name):

    multiscales = MultiScaleImage(
        datasets=datasets,
        axes=generate_axes(),
        version="0.4",
        name=name,
        metadata=MSMetadata(
            method="BIA scripts",
            version="0.1"
        )
    )

    return multiscales


def round_to_sigfigs(x: float, sigfigs: int = 3) -> float:
    """
    Round a float to a specified number of significant figures.
    
    Args:
        x: Number to round
        sigfigs: Number of significant figures to keep (default 3)
    
    Returns:
        Float rounded to specified significant figures
    """
    if x == 0:
        return 0
    return float(f'{x:.{sigfigs}g}')


def generate_dataset_objects(
    start_scales,
    factors,
    path_keys
):
    datasets = [
        DataSet(
            path=path_label,
            coordinateTransformations=[
                CoordinateTransformation(
                    scale = [
                        round_to_sigfigs(start_scale / factor, 3)
                        for (start_scale, factor) in zip(start_scales, factors[n])
                    ],
                    type="scale"
                )
            ]
        )
        for n, path_label in enumerate(path_keys)
    ]

    return datasets
