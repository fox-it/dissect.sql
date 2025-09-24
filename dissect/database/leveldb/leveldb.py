from __future__ import annotations

import os
from io import BytesIO
from itertools import chain
from typing import TYPE_CHECKING, BinaryIO

from dissect.cstruct import u32, u64

from dissect.database.leveldb.c_leveldb import c_leveldb
from dissect.database.utils.protobuf import decode_varint

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

try:
    from cramjam import snappy

    HAS_CRAMJAM = True

except ImportError:
    HAS_CRAMJAM = False


class LevelDB:
    """Google LevelDB implementation.

    References:
        - https://github.com/google/leveldb/blob/main/doc/log_format.md
        - https://github.com/google/leveldb/blob/master/doc/table_format.md
        - https://www.cclsolutionsgroup.com/post/hang-on-thats-not-sqlite-chrome-electron-and-leveldb
    """

    path: Path
    manifests: list[ManifestFile]
    log_files: list[LogFile]
    ldb_files: list[LdbFile]

    def __init__(self, path: Path):
        self.path = path
        self.manifests = []
        self.log_files = []
        self.ldb_files = []

        if not path.exists():
            raise FileNotFoundError(f"Provided path does not exist: {path!r}")

        if not path.is_dir():
            raise NotADirectoryError(f"Provided path is not a directory: {path!r}")

        for file in path.iterdir():
            if not file.is_file():
                continue

            if file.suffix.lower() == ".log":
                self.log_files.append(LogFile(path=file))

            elif file.suffix.lower() in (".ldb", ".sst"):
                self.ldb_files.append(LdbFile(path=file))

            elif file.name.startswith("MANIFEST-"):
                self.manifests.append(ManifestFile(path=file))

        # TODO: Sort log and ldb files based on name

        self.records = list(self._records())

    def __repr__(self) -> str:
        return f"<LevelDB path='{self.path!s}' ldbs={len(self.ldb_files)!r} logs={len(self.log_files)!r}>"

    def _records(self) -> Iterator[Record]:
        """Iterate over all records in this LevelDB."""
        for file in chain(self.ldb_files, self.log_files):
            yield from file.records


class LogFile:
    """Represents a single LevelDB log file."""

    path: Path | None = None
    fh: BinaryIO
    blocks: list[LogBlock]

    def __init__(self, *, path: Path | None = None, fh: BinaryIO | None = None):
        if path:
            self.path = path
            self.fh = path.open("rb")

        elif fh:
            self.fh = fh

        if not path and not fh:
            raise ValueError("LogFile requires one of path or fh")

        self.blocks = list(self._iter_batches())

    def __repr__(self) -> str:
        return f"<LogFile path='{self.path or 'BinaryIO'!s}' blocks={len(self.blocks)}>"

    def _iter_chunks(self) -> Iterator[BytesIO, int]:
        """Yields chunks of 32KB from the logfile file handle."""

        while buf := self.fh.read(c_leveldb.LOG_BLOCK_SIZE):
            yield BytesIO(buf), len(buf)

    def _iter_batches(self) -> Iterator:
        """Yields stitched :class:`LogBlock` instances."""

        chunk_buffer = b""  # TODO: Use MappingStream

        for chunk, size in self._iter_chunks():
            while chunk.tell() < size:
                header = c_leveldb.LogBlockHeader(chunk)

                if header.type == c_leveldb.LogBlockType.FULL:
                    yield LogBlock(None, header, BytesIO(chunk.read(header.size)))

                elif header.type == c_leveldb.LogBlockType.FIRST:
                    chunk_buffer = chunk.read(header.size)

                elif header.type == c_leveldb.LogBlockType.MIDDLE:
                    chunk_buffer += chunk.read(header.size)

                elif header.type == c_leveldb.LogBlockType.LAST:
                    chunk_buffer += chunk.read(header.size)
                    yield LogBlock(None, header, BytesIO(chunk_buffer))

    @property
    def records(self) -> Iterator[Record]:
        """Convenience method to iterate over all blocks for their respective records."""
        for block in self.blocks:
            yield from block.records


class LogBlock:
    """Represents a single LevelDB block."""

    header: c_leveldb.LogBlockHeader
    records: list[c_leveldb.Record]

    type: c_leveldb.LogBlockType

    def __init__(self, fh: BinaryIO, header: c_leveldb.LogBlockHeader | None = None, data: BinaryIO | None = None):
        if header:
            self.header = header
        else:
            self.header = c_leveldb.LogBlockHeader(fh)

        self.type = self.header.type

        if data:
            self.data = data
        else:
            self.data = BytesIO(fh.read(self.header.size))

        self.records = list(self._iter_records())

    def __repr__(self) -> str:
        return f"<LogBlock type={self.header.type.name!r} size={self.header.size!r} crc32c={self.header.crc32c!r}>"

    def _iter_records(self) -> Iterator[Record]:
        while self.data.tell() < self.header.size:
            batch_header = c_leveldb.BatchHeader(self.data)
            for _ in range(batch_header.rec_count):
                yield Record(self.data, batch_header)


class Record:
    """Represents a single LevelDB key/value record pair."""

    state: c_leveldb.RecordState | None = None
    sequence: int | None = None
    key: bytes
    value: bytes
    metadata: bytes

    def __init__(self, fh: BinaryIO | None, batch_header: c_leveldb.BatchHeader | None) -> None:
        self.batch_header = batch_header
        self.fh = fh

        if fh:
            self.header = c_leveldb.Record(fh)
            self.state = self.header.state
            self.key = self.header.key

        if self.state == c_leveldb.RecordState.DELETED:
            self.header.value_len = 0
            self.value = b""
        elif fh:
            self.header.value_len = decode_varint(fh, 5)
            self.value = self.fh.read(self.header.value_len)

        if batch_header:
            self.sequence = batch_header.seq_num

    def __repr__(self) -> str:
        return f"<Record sequence={self.sequence!r} state={self.state.name!r} key={self.key!r} value={self.value!r}>"


class LdbFile:
    """Represents a single LevelDB ``.ldb`` file."""

    path: Path | None = None
    fh: BinaryIO
    records: Iterator[Record]
    footer: c_leveldb.LdbFooter

    def __init__(self, *, path: Path | None = None, fh: BinaryIO | None = None):
        if path:
            self.path = path
            self.fh = path.open("rb")

        elif fh:
            self.fh = fh

        if not path and not fh:
            raise ValueError("LdbFile requires one of path or fh")

        self.fh.seek(-c_leveldb.LDB_FOOTER_SIZE, os.SEEK_END)
        self.footer = c_leveldb.LdbFooter(self.fh)

        self.fh.seek(-8, os.SEEK_END)
        self.footer.magic = u64(self.fh.read())

        if self.footer.magic != c_leveldb.LDB_MAGIC:
            raise ValueError(f"Invalid LevelDB footer magic {self.footer.magic!r}")

        self.meta_index_block = LdbMetaIndexBlock(self.fh, self.footer.meta_index_handle)
        self.index_block = LdbIndexBlock(self.fh, self.footer.index_handle)

        self._records = []

    def __repr__(self) -> str:
        return f"<LdbFile path='{self.path or 'BinaryIO'!s}' records={len(self.records)}>"

    @property
    def records(self) -> Iterator[Record]:
        if self._records:
            yield from self._records

        for _, handle in self.index_block.entries:
            block = LdbBlock(self.fh, handle)
            for block_entry, _ in block.entries:
                record = Record(None, None)
                record.metadata = block_entry.key[-8:]
                record.sequence = u64(record.metadata) >> 8

                if len(block_entry.key) > 8:
                    record.state = (
                        c_leveldb.RecordState.DELETED if block_entry.key[-8] == 0 else c_leveldb.RecordState.LIVE
                    )
                else:
                    record.state = c_leveldb.RecordState.UNKNOWN

                record.key = block_entry.key[:-8]
                record.value = block_entry.value
                self._records.append(record)
                yield record


class LdbBlock:
    """Represents a single LevelDB ``.ldb`` file block.

    Unlike :class:`LogBlock`, blocks in ``.ldb`` files do not have a fixed length::

        | block_entry[n] | <- can be compressed
        | -------------- |
        | restart_array  | <- can be compressed
        | -------------- |
        | trailer        |

    """

    def __init__(self, fh: BinaryIO, block_handle: c_leveldb.BlockHandle):
        self.block_handle = block_handle

        fh.seek(block_handle.offset)
        self.raw_data = fh.read(block_handle.length)
        self.trailer = c_leveldb.BlockTrailer(fh.read(c_leveldb.LDB_BLOCK_TRAILER_SIZE))

        self.offset = self.block_handle.offset
        self.compression = self.trailer.compression
        self.crc32c = self.trailer.crc32c
        self.size = self.block_handle.length
        self.size_decompressed = self.size

        if len(self.raw_data) != block_handle.length or len(self.trailer.dumps()) != c_leveldb.LDB_BLOCK_TRAILER_SIZE:
            raise ValueError(f"Unable to read full LdbBlock at offset {block_handle.offset}")

        if self.trailer.compression == c_leveldb.CompressionType.SNAPPY:
            if not HAS_CRAMJAM:
                raise ImportError(
                    "Unable to decompress snappy LdbBlock: missing dependency cramjam, install with 'pip install dissect.database[leveldb]'"  # noqa: E501
                )
            try:
                self.data = snappy.decompress_raw(self.raw_data)
                self.size_decompressed = self.data.len()
            except snappy.DecompressionError as e:
                raise ValueError("Unable to decompress LdbBlock: snappy decompression failed") from e
        else:
            self.data = BytesIO(self.raw_data)

        # Read restart pointer is stored after all block entries.
        self.data.seek(-4, os.SEEK_END)
        self._restart_count = u32(self.data.read())
        self._restart_offset = self.size_decompressed - (self._restart_count + 1) * 4

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} offset={self.offset!r} size={self.size!r} crc32c={self.crc32c.hex()!r} compression={self.compression.name!r} size_decompressed={self.size_decompressed!r}>"  # noqa: E501

    def restart_offset(self, idx: int) -> int:
        offset = self._restart_offset + (idx * 4)
        self.data.seek(offset)
        return u32(self.data.read(4), sign=True)

    @property
    def entries(self) -> Iterator[tuple[c_leveldb.BlockEntry, c_leveldb.BlockHandle]]:
        offset = self.restart_offset(0)
        self.data.seek(offset)

        if offset >= self._restart_offset:
            raise ValueError("Reading start of entry past the start of restart offset")

        key = b""
        while self.data.tell() < self._restart_offset:
            entry = c_leveldb.BlockEntry(self.data)

            if entry.shared_len > len(key):
                raise ValueError("Shared key length is longer than the previous key")

            key = key[: entry.shared_len] + self.data.read(entry.non_shared_len)
            entry.key = key
            entry.value = self.data.read(entry.value_len)

            handle = c_leveldb.BlockHandle(entry.value)

            yield entry, handle


class LdbMetaIndexBlock(LdbBlock):
    """Represents a single LevelDB ``.ldb`` meta index block."""


class LdbIndexBlock(LdbBlock):
    """Represents a single LevelDB ``.ldb`` index block."""


class ManifestFile:
    """Represents a single ``MANIFEST-*`` file."""

    path: Path | None
    fh: BinaryIO

    def __init__(self, *, path: Path | None = None, fh: BinaryIO | None = None):
        if path:
            self.path = path
            self.fh = path.open("rb")
        elif fh:
            self.fh = fh

        if not path and not fh:
            raise ValueError("ManifestFile requires one of path or fh")

    def __repr__(self) -> str:
        return f"<ManifestFile path='{self.path or 'BinaryIO'!s}'>"
