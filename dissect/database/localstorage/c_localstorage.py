from __future__ import annotations

from dissect.cstruct import cstruct

from dissect.database.utils.protobuf import ProtobufVarint, ProtobufVarint32

# References:
# - https://github.com/chromium/chromium/blob/main/components/services/storage/dom_storage/local_storage_database.proto
localstorage_def = """
struct LocalStorageAreaWriteMetaData {
    uint8       lm_type;
    varint      last_modified;

    uint8       sb_type;
    varint      size_bytes;
};

struct LocalStorageAreaAccessMetaData {
    uint8       la_type;
    varint      last_accessed;
};
"""

c_localstorage = cstruct()
c_localstorage.add_custom_type("varint", ProtobufVarint, size=None, alignment=1, signed=False)
c_localstorage.add_custom_type("varint64", ProtobufVarint, size=None, alignment=1, signed=False)
c_localstorage.add_custom_type("varint32", ProtobufVarint32, size=None, alignment=1, signed=False)
c_localstorage.load(localstorage_def)
