class Error(Exception):
    """Base class for exceptions for this module.
    It is used to recognize errors specific to this module"""

    pass


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
