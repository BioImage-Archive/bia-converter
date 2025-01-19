bia-converter
=============

Use
---

Currently provides a single CLI command `convert`, which takes as arguments the UUID of an
existing ImageRepresentation, and the type of a second representation. Invoked like this:

    poetry run bia-converter convert b532b633-9c29-4779-ac2b-7e6c6334ea5f THUMBNAIL

It will determine if the conversion is supported, and, if it is, perform the conversion and
upload the result to an S3 location configured by environmental variables.

The code supports conversion of multiple input files to a single output image (e.g. a stack
of TIFF files to a single OME-Zarr).

Setup
-----

TODO:

* Allow overrides when units are not set correctly
* More broadly, support passing in conversion options
* Support routes where we can convert *some* subtypes of a representation, e.g. we can convert UPLOADED_BY_SUBMITTOR MRC files to PNG
* Modernise / share the OME-Zarr reading code


zarr2zarr
---------

https://uk1s3.embassy.ebi.ac.uk/bia-integrator-data/EMPIAR-10392/IM1/IM1.zarr/0