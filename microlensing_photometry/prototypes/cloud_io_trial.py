# Cloud IO Trial Code

# The purpose of this code is to prototype accessing data stored in AWS S3 buckets
# rather than local disk.
# This is based on the Astropy tutorial:
# https://docs.astropy.org/en/stable/io/fits/usage/cloud.html

from os import environ, path
from astropy.io import fits

# User AWS credentials need to be provided; for security these
# are served as environment variables
fsspec_kwargs = {"key": environ['AWS_SECRET_KEY_ID'],
                 "secret": environ['AWS_SECRET']}

# Load an image file
s3_uri = path.join(environ['AWS_BUCKET'],
                   'software', 'test_images', 'AT2024kwu_ip',
                   'tfn1m001-fa11-20240930-0341-e91.fits')
with fits.open(s3_uri, fsspec_kwargs=fsspec_kwargs) as hdul:

    # Access the image header
    print(hdul[0].header)

    # Summarize the FITS extensions
    for i,hdu in enumerate(hdul):
        print('Extension ' + str(i) + ' ' + hdu.name)

    # Show image data
    print(hdul[0].data)

    # Show source catalog
    print(hdul[1].data)