#!/usr/bin/env python3
"""Generate minimal compressed DICOM test fixtures.

Requires: pydicom, numpy

Usage:
    pip install pydicom numpy
    python tests/generate_fixtures.py

The generated fixtures are checked into the repository.
"""

import numpy as np
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import (
    ExplicitVRLittleEndian,
    RLELossless,
    generate_uid,
)


def make_base_dataset() -> Dataset:
    """Create a base 8x8 monochrome dataset with white pixels."""
    ds = Dataset()
    ds.Rows = 8
    ds.Columns = 8
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.NumberOfFrames = 1
    ds.PixelData = np.full((8, 8), 255, dtype=np.uint8).tobytes()
    return ds


def save_compressed(ds: Dataset, transfer_syntax_uid: str, path: str):
    """Compress and save the dataset."""
    file_meta = FileMetaDataset()
    file_meta.FileMetaInformationVersion = b"\x00\x01"
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = "1.2.3.4"

    fds = FileDataset("", ds, file_meta=file_meta, preamble=b"\x00" * 128)
    fds.is_little_endian = True
    fds.is_implicit_VR = False

    # Compress using pydicom's compress utility
    fds.compress(transfer_syntax_uid)

    # Ensure group length is present for dicom-object compatibility
    fds.file_meta.ensure_file_meta()

    fds.save_as(path, enforce_file_format=True)
    print(f"Saved: {path} (transfer syntax: {transfer_syntax_uid})")


if __name__ == "__main__":
    base = make_base_dataset()

    # RLE Lossless -- natively supported by pydicom
    save_compressed(base, RLELossless, "tests/fixtures/rle_lossless_8x8.dcm")
