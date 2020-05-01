"""Tools for processing files."""

import os
from shutil import make_archive
from shutil import unpack_archive


def zip(srcdir, destdir):
    make_archive(srcdir, 'zip', destdir)


def unzip(filename, extractdir):
    try:
        os.mkdir(extractdir)
    except FileExistsError:
        pass
    unpack_archive(filename, extract_dir=extractdir)


def merge_files(input_files, output_file):
    with open(output_file, "wb") as outfile:
        for f in input_files:
            with open(f, "rb") as infile:
                outfile.write(infile.read())
