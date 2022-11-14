"""
For calculation purposes, we assume a maximum number of files that can be in the database as 99.
It is only an assumed value. So, there will not be any limitation for the total file number in the database.
"""
MAX_NUM_OF_FILES = 99
MAX_NUM_OF_FIELDS = 12
MAX_TYPE_NAME_LENGTH = 20
MAX_FIELD_NAME_LENGTH = 20
MAX_FIELD_VALUE_LENGTH = 20
RECORD_PER_PAGE = 8
PAGE_PER_FILE = 8
MAX_FILE_INDEX_LENGTH = 2
MAX_FILE_HEADER_LENGTH = MAX_FILE_INDEX_LENGTH + 1 + 1 + MAX_TYPE_NAME_LENGTH + 1 + MAX_FIELD_NAME_LENGTH + 1 + MAX_NUM_OF_FIELDS*MAX_FIELD_NAME_LENGTH + 1 + MAX_NUM_OF_FIELDS*MAX_FIELD_NAME_LENGTH + 1 + 1
#MAX_FILE_HEADER_LENGTH = MAX_TYPE_NAME_LENGTH + MAX_FIELD_NAME_LENGTH + MAX_NUM_OF_FIELDS*MAX_FIELD_NAME_LENGTH + MAX_NUM_OF_FIELDS*MAX_FIELD_NAME_LENGTH + 1 + 2
MAX_PAGE_HEADER_LENGTH = 1 + 1 + 1 + 1
MAX_RECORD_HEADER_LENGTH = 1 + 1 + 1 + 1 + 1
RECORD_SIZE = MAX_FIELD_VALUE_LENGTH*MAX_NUM_OF_FIELDS
PAGE_SIZE = RECORD_SIZE*RECORD_PER_PAGE + MAX_PAGE_HEADER_LENGTH
FILE_SIZE = PAGE_SIZE*PAGE_PER_FILE + MAX_FILE_HEADER_LENGTH
PAGE_LOCS = [8, 35, 62, 89, 116, 143, 170, 197]
BUFFER = "                                                                                                                                                                                                                                                "