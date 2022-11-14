# Author 1: Altay Acar - 2018400084
# Author 2: Engin Oguzhan Senol - 2020400324
# CMPE321 - Introduction to Database Systems - Project 4
# 27.05.2022

import constants
import time
import csv
import os


def logger(file, log_message, status):
    log_module = csv.writer(file)
    if status:
        log = [int(time.time()), log_message, "success"]
    else:
        log = [int(time.time()), log_message, "failure"]
    log_module.writerow(log)

def bufferize(string):
    offset = constants.MAX_FIELD_VALUE_LENGTH - len(string)
    buffered = string
    for i in range(offset):
        buffered = buffered + " "
    return buffered

def format_for_file(list):
    entry = ""
    for element in list:
        entry = entry + bufferize(element)
    for i in range(len(entry), (constants.MAX_NUM_OF_FIELDS*constants.MAX_FIELD_NAME_LENGTH)):
        entry = entry + " "
    return entry

def read_by_twenty(file, cursor):
    with open(file, "r") as f:
        f.seek(cursor)
        element = f.read(20)
    return element

def tokenize_file_entry(file, cursor):
    l = []
    with open(file, "r") as f:
        f.seek(cursor)
        for i in range(cursor, cursor+240, 20):
            temp_item = read_by_twenty(file, i)
            item = temp_item.strip()
            l.append(item)
    l = list(filter(None, l))
    return l

def get_record_loc(page_id, record_id):
    r_header_factor = record_id
    r_factor = record_id-1
    page_factor = (page_id-1)*8
    return (constants.MAX_FILE_HEADER_LENGTH + constants.MAX_PAGE_HEADER_LENGTH*page_id + constants.MAX_RECORD_HEADER_LENGTH*(page_factor+r_header_factor) + constants.RECORD_SIZE*(page_factor+r_factor) + (page_id-1))

def create_type_file(type_name, primary_key_order, field_names, field_types, file_name, index):
    primary_key = field_names[primary_key_order-1]
    type_file = open(file_name, "w")
    if index < 10:
        f_index = "0" + str(index)
        type_file.write(f_index)
    else:
        type_file.write(str(index))
    type_file.write('\n')
    type_file.write("0")
    type_file.write('\n')
    type_file.write(bufferize(type_name))
    type_file.write('\n')
    type_file.write(bufferize(primary_key))
    type_file.write('\n')
    type_file.write(format_for_file(field_names))
    type_file.write('\n')
    type_file.write(format_for_file(field_types))
    type_file.write('\n')
    type_file.write('\n')
    for i in range(constants.PAGE_PER_FILE):
        page_index = i+1
        type_file.write(str(page_index))
        type_file.write('\n')
        type_file.write("0")
        type_file.write('\n')
        for j in range(constants.RECORD_PER_PAGE):
            record_index = str(j+1)
            type_file.write(record_index)
            type_file.write('\n')
            type_file.write("0")
            type_file.write('\n')
            type_file.write(constants.BUFFER)
            if i != 9 or j != 9:
                type_file.write('\n')
        if i != 9:
            type_file.write('\n')
    return True

def create_additional_file(index, type_name, primary_key, field_names, field_types, file_name):
    type_file = open(file_name, "w")
    if index < 10:
        f_index = "0" + str(index)
        type_file.write(f_index)
    else:
        type_file.write(str(index))
    type_file.write('\n')
    type_file.write("0")
    type_file.write('\n')
    type_file.write(bufferize(type_name))
    type_file.write('\n')
    type_file.write(bufferize(primary_key))
    type_file.write('\n')
    type_file.write(format_for_file(field_names))
    type_file.write('\n')
    type_file.write(format_for_file(field_types))
    type_file.write('\n')
    type_file.write('\n')
    for i in range(constants.PAGE_PER_FILE):
        page_index = i+1
        type_file.write(str(page_index))
        type_file.write('\n')
        type_file.write("0")
        type_file.write('\n')
        for j in range(constants.RECORD_PER_PAGE):
            record_index = str(j+1)
            type_file.write(record_index)
            type_file.write('\n')
            type_file.write("0")
            type_file.write('\n')
            type_file.write(constants.BUFFER)
            if i != 9 or j != 9:
                type_file.write('\n')
        if i != 9:
            type_file.write('\n')
    return True

def get_file_header(file):
    with open(file, "r") as f:
        lines = f.readlines()
        index = int(lines[0].split()[0])
        temp = int(lines[1].split()[0])
        is_full = False
        if temp:
            is_full = True
        type = lines[2].strip()
        primary_key = lines[3].strip()
        fields = tokenize_file_entry(file, 47)
        field_types = tokenize_file_entry(file, 288)
        header = {
            "index": index,
            "is_full": is_full,
            "type": type,
            "primary_key": primary_key,
            "fields": fields,
            "field_types": field_types
        }
        return header

def get_file_index(file):
    header = get_file_header(file)
    return header["index"]

def is_file_full(file):
    header = get_file_header(file)
    return header["is_full"]

def get_file_type(file):
    header = get_file_header(file)
    return header["type"]

def get_file_primary_key(file):
    header = get_file_header(file)
    return header["primary_key"]

def get_file_fields(file):
    header = get_file_header(file)
    return header["fields"]

def get_file_field_types(file):
    header = get_file_header(file)
    return header["field_types"]

def create_record_entry(fields):
    return format_for_file(fields)

def is_page_full(file_name, page_id):
    with open(file_name, 'r') as file:
        lines = file.readlines()
    is_page_full = int(lines[constants.PAGE_LOCS[page_id-1]].split()[0])
    if is_page_full:
        return True
    else:
        return False

def is_record_full(file_name, record_loc):
    with open(file_name, 'r') as f:
        offset = record_loc-2
        f.seek(offset)
        val = f.read(1)
        is_full = int(val)
        return is_full

def find_first_available_page(file_name):
    #returns the id of the first available page, if there is none returns 0
    with open(file_name, 'r') as file:
        lines = file.readlines()
    num_of_pages = 0
    for i in range(len(constants.PAGE_LOCS)):
        if len(lines) < constants.PAGE_LOCS[i]:
            num_of_pages = i
    if len(lines) > constants.PAGE_LOCS[7]:
        num_of_pages = 8
    for i in range(num_of_pages):
        page_id = i+1
        if not is_page_full(file_name, page_id):
            return page_id
    return 0

def find_first_available_record(file_name, page_id):
    #returns the id of the first available page, always given a page that has an available record
    with open(file_name, 'r') as file:
        lines = file.readlines()
    page_loc = constants.PAGE_LOCS[page_id-1]
    for i in range(page_loc+1, page_loc+24, 3):
        rec_status = lines[i+1].strip()
        is_rec_full = int(rec_status)
        if not is_rec_full:
           return int(lines[i].strip())
    return 0

def record_entry(file, page_id, record_id, entry):
    record_loc = get_record_loc(page_id, record_id)
    with open(file, "r+") as f:
        f.seek(record_loc)
        f.write(entry)

def make_record_full(file, page_id, record_id):
    r_header_factor = record_id
    r_factor = record_id-1
    page_factor = (page_id-1)*8
    with open(file, "r+") as f:
        f.seek(constants.MAX_FILE_HEADER_LENGTH + constants.MAX_PAGE_HEADER_LENGTH*page_id + constants.MAX_RECORD_HEADER_LENGTH*(page_factor+r_header_factor) + constants.RECORD_SIZE*(page_factor+r_factor) - 2 + (page_id-1))
        f.write("1")

def make_record_empty(file, page_id, record_id):
    r_header_factor = record_id
    r_factor = record_id-1
    page_factor = (page_id-1)*8
    with open(file, "r+") as f:
        f.seek(constants.MAX_FILE_HEADER_LENGTH + constants.MAX_PAGE_HEADER_LENGTH*page_id + constants.MAX_RECORD_HEADER_LENGTH*(page_factor+r_header_factor) + constants.RECORD_SIZE*(page_factor+r_factor) - 2 + (page_id-1))
        f.write("0")

def make_page_full(file, page_id):
    page_factor = page_id-1
    with open(file, "r+") as f:
        f.seek(constants.MAX_FILE_HEADER_LENGTH + constants.MAX_PAGE_HEADER_LENGTH*page_id + constants.MAX_RECORD_HEADER_LENGTH*(8*page_factor) + constants.RECORD_SIZE*(8*page_factor) - 1 + page_factor)
        f.write("1")

def make_page_empty(file, page_id):
    page_factor = page_id-1
    with open(file, "r+") as f:
        f.seek(constants.MAX_FILE_HEADER_LENGTH + constants.MAX_PAGE_HEADER_LENGTH*page_id + constants.MAX_RECORD_HEADER_LENGTH*(8*page_factor) + constants.RECORD_SIZE*(8*page_factor) - 1 + page_factor)
        f.write("0")

def make_file_full(file):
    with open(file, "r+") as f:
        f.seek(3)
        f.write("1")

def make_file_empty(file):
    with open(file, "r+") as f:
        f.seek(3)
        f.write("0")

def free_record(file, offset):
    with open(file, "r+") as f:
        f.seek(offset)
        f.write(constants.BUFFER)

def add_record(file, fields):
    #File with available empty space is given as parameter
    entry = create_record_entry(fields)
    #Find the first available page in the file
    page_id = find_first_available_page(file)
    #Find the first available record in that page
    record_id = find_first_available_record(file, page_id)
    #Add file to the found available record slot
    record_entry(file, page_id, record_id, entry)
    file_header = get_file_header(file)
    file_id = file_header["index"]
    record_index = [file_id, page_id, record_id]
    make_record_full(file, page_id, record_id)
    #Check if the page is full
    page_avl = find_first_available_record(file, page_id)
    if not page_avl:
        #If full, change the is_full field in the page header
        make_page_full(file, page_id)
        #If full, check if the file is full after entry
        file_avl = find_first_available_page(file)
        if not file_avl:
            #If the file is also full, change the is_full field in the page header
            make_file_full(file)
    return [True, record_index]

def delete_record(file_name, record_index, key):
    formatted_file = "./" + file_name
    if os.path.exists(formatted_file):
        with open(file_name, 'r+') as f:
            #File found
            file_id = record_index[0]
            page_id = record_index[1]
            record_id = record_index[2]
            #Double check if the file_id's are matching with each other
            header = get_file_header(file_name)
            if header["index"] == file_id:
                #File is correct
                #Go to the given record slot
                record_loc = get_record_loc(page_id, record_id)
                #Check if that record is full by the record header
                if is_record_full(file_name, record_loc):
                    #If full, check if the primary key's are matching
                    primary_key = header["primary_key"]
                    fields = header["fields"]
                    count = 0
                    for field in fields:
                        if primary_key == field:
                            break
                        count = count + 1
                    record = tokenize_file_entry(file_name, record_loc)
                    record_key = record[count]
                    if key == record_key:
                        #If the keys are matching, delete the record as expected
                        free_record(file_name, record_loc)
                        make_record_empty(file_name, page_id, record_id)
                        if is_page_full(file_name, page_id):
                            make_page_empty(file_name, page_id)
                        if is_file_full(file_name):
                            make_file_empty(file_name)
                        return True
                    else:
                        #Keys are not matching
                        return False
                else:
                    #Record slot is empty
                    return False
            else:
                #File is false
                return False
    else:
        #Trying to search for a record in a non-existing file
        return False

def get_all_records(file_name):
    records = []
    with open(file_name, 'r') as file:
        for page in range(1, constants.PAGE_PER_FILE+1):
            for record in range(1, constants.RECORD_PER_PAGE+1):
                record_loc = get_record_loc(page, record)
                if is_record_full(file_name, record_loc):
                    record_entry = tokenize_file_entry(file_name, record_loc)
                    formatted_output = ""
                    for item in record_entry:
                        formatted_output = formatted_output + item + " "
                    records.append(formatted_output)
    return records

def update_record(file_name, record_index, upd_fields):
    formatted_file = "./" + file_name
    if os.path.exists(formatted_file):
        with open(file_name, 'r+') as f:
            #File found
            file_id = record_index[0]
            page_id = record_index[1]
            record_id = record_index[2]
            #Double check if the file_id's are matching with each other
            header = get_file_header(file_name)
            if header["index"] == file_id:
                #File is correct
                #Go to the given record slot
                record_loc = get_record_loc(page_id, record_id)
                #Check if that record is full by the record header
                if is_record_full(file_name, record_loc):
                    #If full, check if the primary key's are matching
                    primary_key = header["primary_key"]
                    fields = header["fields"]
                    count = 0
                    for field in fields:
                        if primary_key == field:
                            break
                        count = count + 1
                    record = tokenize_file_entry(file_name, record_loc)
                    record_key = record[count]
                    upd_record_key = upd_fields[count]
                    if upd_record_key == record_key:
                        #If the keys are matching, update the record as expected
                        entry = create_record_entry(upd_fields)
                        record_entry(file_name, page_id, record_id, entry)
                        return True
                    else:
                        #Keys are not matching
                        return False
                else:
                    #Record slot is empty
                    return False
            else:
                #File is false
                return False
    else:
        #Trying to search for a record in a non-existing file
        return False

def search_record(file_name, record_index):
    with open(file_name, 'r') as file:
        page_id = record_index[1]
        record_id = record_index[2]
        record_loc = get_record_loc(page_id, record_id)
        fields = tokenize_file_entry(file_name, record_loc)
        output = ""
        for item in fields:
            output = output + item + " "
        return output

def filter_record(file_names, record_indexes):
    result = []
    for i in range(len(record_indexes)):
        with open(file_names[i], 'r') as file:
            item = record_indexes[i]
            page_id = item[1]
            record_id = item[2]
            record_loc = get_record_loc(page_id, record_id)
            fields = tokenize_file_entry(file_names[i], record_loc)
            formatted_entry = ""
            for item in fields:
                formatted_entry = formatted_entry + item + " "
            result.append(formatted_entry)
    return result