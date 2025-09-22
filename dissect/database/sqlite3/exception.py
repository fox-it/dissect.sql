from __future__ import annotations

from dissect.database.exception import Error


class InvalidDatabase(Error):
    pass


class InvalidPageNumber(Error):
    pass


class InvalidPageType(Error):
    pass


class InvalidSQL(Error):
    pass


class NoCellData(Error):
    pass


class NoWriteAheadLog(Error):
    pass
