from dissect import cstruct

sqlite3_def = """
#define PAGE_FLAG_INTKEY      0x01
#define PAGE_FLAG_ZERODATA    0x02
#define PAGE_FLAG_LEAFDATA    0x04
#define PAGE_FLAG_LEAF        0x08

#define PAGE_TYPE_INTERIOR_INDEX    PAGE_FLAG_ZERODATA
#define PAGE_TYPE_INTERIOR_TABLE    PAGE_FLAG_INTKEY | PAGE_FLAG_LEAFDATA
#define PAGE_TYPE_LEAF_INDEX        PAGE_FLAG_ZERODATA | PAGE_FLAG_LEAF
#define PAGE_TYPE_LEAF_TABLE        PAGE_FLAG_INTKEY | PAGE_FLAG_LEAFDATA | PAGE_FLAG_LEAF

struct header {
    char    magic[16];
    uint16  page_size;
    uint8   write_version;
    uint8   read_version;
    uint8   reserved_size;
    uint8   max_embedded_payload_fraction;
    uint8   min_embedded_payload_fraction;
    uint8   leaf_payload_fraction;
    uint32  change_counter;
    uint32  page_count;
    uint32  first_freelist_page;
    uint32  freelist_page_count;
    uint32  schema_cookie;
    uint32  schema_format_number;
    uint32  page_cache_size;
    uint32  largest_root_btree_page;
    uint32  text_encoding;
    uint32  user_version;
    uint32  incremental_vacuum_mode;
    uint32  application_id;
    char    reserved1[20];
    uint32  version_valid_for_number;
    uint32  sqlite_version_number;
};

struct page_header {
    uint8   flags;
    uint16  first_freeblock;
    uint16  cell_count;
    uint16  cell_start;
    uint8   fragmented_free_bytes;
};

struct wal_header {
    uint32  magic;
    uint32  version;
    uint32  page_size;
    uint32  checkpoint_sequence_number;
    uint32  salt1;
    uint32  salt2;
    uint32  checksum1;
    uint32  checksum2;
};

struct wal_frame {
    uint32  page_number;
    uint32  page_count;
    uint32  salt1;
    uint32  salt2;
    uint32  checksum1;
    uint32  checksum2;
};
"""

c_sqlite3 = cstruct.cstruct(endian=">")
c_sqlite3.load(sqlite3_def)

ENCODING = {
    1: "utf-8",
    2: "utf-16-le",
    3: "utf-16-be",
}

PAGE_TYPES = {
    c_sqlite3.PAGE_TYPE_INTERIOR_INDEX: "PAGE_TYPE_INTERIOR_INDEX",
    c_sqlite3.PAGE_TYPE_INTERIOR_TABLE: "PAGE_TYPE_INTERIOR_TABLE",
    c_sqlite3.PAGE_TYPE_LEAF_INDEX: "PAGE_TYPE_LEAF_INDEX",
    c_sqlite3.PAGE_TYPE_LEAF_TABLE: "PAGE_TYPE_LEAF_TABLE",
}

SERIAL_TYPES = {
    0: lambda fh: None,
    1: c_sqlite3.uint8,
    2: c_sqlite3.uint16,
    3: c_sqlite3.uint24,
    4: c_sqlite3.uint32,
    5: c_sqlite3.uint48,
    6: c_sqlite3.uint64,
    7: c_sqlite3.double,
    8: lambda fh: 0,
    9: lambda fh: 1,
}

SQLITE3_HEADER_MAGIC = b"SQLite format 3\x00"

WAL_HEADER_MAGIC_LE = 0x377F0682
WAL_HEADER_MAGIC_BE = 0x377F0683
WAL_HEADER_MAGIC = {WAL_HEADER_MAGIC_LE, WAL_HEADER_MAGIC_BE}
