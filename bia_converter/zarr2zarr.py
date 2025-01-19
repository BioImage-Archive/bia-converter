from pathlib import Path

import zarr
import rich
import typer

from .proxyimage import ome_zarr_image_from_ome_zarr_uri
from .omezarrgen import (
    rechunk_and_save_array,
    create_ome_zarr_metadata,
    create_omero_metadata_object,
    downsample_array_and_write_to_dirpath
)


app = typer.Typer()


@app.command()
def zarr_group_info(zarr_uri):

    group = zarr.open_group(zarr_uri)
    for k in group.array_keys():
        ar = group[k]
        rich.print(f"Key: {k}, shape: {ar.shape}, chunks: {ar.chunks}")


@app.command()
def ome_zarr_info(ome_zarr_uri):
    im = ome_zarr_image_from_ome_zarr_uri(ome_zarr_uri)

    rich.print(im)


@app.command()
def zarr2zarr(ome_zarr_uri: str, output_base_dirpath: Path):

    target_chunks = [1, 1, 64, 64, 64]
    downsample_factors = [1, 1, 2, 2, 2]
    coordinate_scales = [1, 1, 2e-8, 1e-8, 1e-8]
    n_pyramid_levels = 3

    output_array_keys = [str(i) for i in range(n_pyramid_levels)]

    # Rechunk the base of the pyramid
    # FIXME - path key for base of incoming pyramid is not always '0', just usually
    input_array_uri = ome_zarr_uri + '/0'
    output_dirpath = output_base_dirpath / '0'
    if not output_dirpath.exists():
        rechunk_and_save_array(input_array_uri, output_dirpath, target_chunks)

    # Regenerate the rest of the period by downsampling
    for level in range(n_pyramid_levels - 1):
        input_array_dirpath = output_base_dirpath / output_array_keys[level] 
        output_array_dirpath = output_base_dirpath / output_array_keys[level+1]
        if not output_array_dirpath.exists():
            rich.print(f"Downsampling from {input_array_dirpath} to {output_array_dirpath}")
            downsample_array_and_write_to_dirpath(
                str(input_array_dirpath),
                output_array_dirpath,
                downsample_factors,
                target_chunks
            )

    # Create and write the OME-Zarr metadata    
    ome_zarr_metadata = create_ome_zarr_metadata(str(output_base_dirpath), "test_name", coordinate_scales)
    group = zarr.open_group(output_base_dirpath)
    group.attrs.update(ome_zarr_metadata.model_dump(exclude_unset=True)) # type: ignore




if __name__ == "__main__":
    app()