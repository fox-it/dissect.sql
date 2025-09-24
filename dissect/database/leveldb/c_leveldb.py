from __future__ import annotations

from dissect.cstruct import cstruct

from dissect.database.utils.protobuf import ProtobufVarint, ProtobufVarint32

leveldb_def = """
/*
 * LevelDB log file structures.
 */

#define LOG_BLOCK_SIZE                  0x8000
#define LOG_ENTRY_HEADER_SIZE           7

enum LogBlockType : uint8 {
    FULL            = 1,
    FIRST           = 2,
    MIDDLE          = 3,
    LAST            = 4,
};

struct LogBlockHeader {
    uint32          crc32c;
    uint16          size;               // size of the first record
    LogBlockType    type;
};

struct BatchHeader {
    uint64          seq_num;
    uint32          rec_count;
};

enum RecordState : uint8 {
    DELETED         = 0,
    LIVE            = 1,
    UNKNOWN         = 2,
};

// TODO: Rename to LogRecord
struct Record {
    RecordState     state;
    varint          key_len;
    char            key[key_len];
    // varint       value_len;          // if state != DELETED
    // char         value[value_len];   // if state != DELETED
};


/*
 * LevelDB .ldb file structures.
 */
#define LDB_MAGIC                       0xdb4775248b80fb57
#define LDB_FOOTER_SIZE                 48
#define LDB_BLOCK_TRAILER_SIZE          5

struct BlockHandle {
    varint          offset;             // varint64
    varint          length;             // varint64
};

struct BlockEntry {
    varint32        shared_len;
    varint32        non_shared_len;
    varint32        value_len;
    char            key[0];             // shared key computed at runtime
    char            value[0];           // value_len read at runtime
};

enum CompressionType : uint8 {
    NONE            = 0,
    SNAPPY          = 1,
};

struct BlockTrailer {
    CompressionType compression;
    char            crc32c[4];
};

struct LdbFooter {
    BlockHandle     meta_index_handle;
    BlockHandle     index_handle;
    // char         padding[40-sizeof(meta_index_handle)-sizeof(index_handle)];
    // char         magic[8];
};
"""

c_leveldb = cstruct(endian="<")
c_leveldb.add_custom_type("varint", ProtobufVarint, size=None, alignment=1, signed=False)
c_leveldb.add_custom_type("varint64", ProtobufVarint, size=None, alignment=1, signed=False)
c_leveldb.add_custom_type("varint32", ProtobufVarint32, size=None, alignment=1, signed=False)
c_leveldb.load(leveldb_def)
