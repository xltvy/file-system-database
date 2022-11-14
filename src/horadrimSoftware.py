# Author 1: Altay Acar - 2018400084
# Author 2: Engin Oguzhan Senol - 2020400324
# CMPE321 - Introduction to Database Systems - Project 4
# 27.05.2022

from venv import create
import constants
import bplustree
import sys
import json
from horadrimUtils import *
import csv
import os.path
import os

bplustree_dict = {}

input_file_path = sys.argv[1]
output_file_path = sys.argv[2]

input_file = open(input_file_path, 'r')
output_file = open(output_file_path, 'a')

commands = input_file.readlines()
input_file.close()

if os.path.exists("./horadrimLog.csv"):
    logFile = open('horadrimLog.csv', 'a')
else:
    logFile = open('horadrimLog.csv', 'a')
    log_module = csv.writer(logFile)
    header = ["occurrence", "operation", "status"]
    log_module.writerow(header)

"""
1- Global B+ Tree dict is already initialized
2- Retrieve each B+ tree object from the current directory using pickle
   |__ 2.1- While retrieving check each file's name and store it into a temporary variable
   |__ 2.2- Name each B+ tree object that is about to be retrieved from that temporary variable name in the appropriate naming convention
   |__ 2.3- Store each retrieved b+ tree's type name alongside
3- Add each retrieved B+ tree to the global B+ tree dict using the type name as the key and the tree as the value
"""
tree_files = []
cond = "-tree.json"
for file in os.listdir(os.getcwd()):
    if file.endswith(cond):
        tree_files.append(str(file))
for f in tree_files:
    typetree = bplustree.BPlusTree()
    type_name = f[:-10]
    with open(f, "r") as jsonFile:
        data = json.load(jsonFile)
    leafs = data["Leafs"]
    for leaf in leafs:
        for node in leaf:
            for key in leaf[node]:
                typetree.insert(key, leaf[node][key])
    bplustree_dict[type_name] = typetree


for command in commands:
    tokenized = command.split()
    operation = tokenized[0]
    data = tokenized[1]

    if operation == "create":
        if data == "type":
            #CREATE TYPE
            if len(tokenized) > 4:
                type_name = tokenized[2]
                num_of_fields = int(tokenized[3])
                primary_key_order = int(tokenized[4])
                factor = 5+num_of_fields*2
                field_names = []
                field_types = []
                if len(tokenized) == factor:
                    with open("types.json", "r") as jsonFile:
                        type_data = json.load(jsonFile)
                    types = type_data["types"]
                    if type_name in types:
                        #Cannot create an already existing type
                        #LOG FAILURE
                        logger(logFile, command.strip(), False)
                    else:
                        """
                        1- Create a global B+ tree object for that type
                        2- Add the newly created B+ tree object to the dict using the type name as key and B+ tree as value
                        """
                        bptree = bplustree.BPlusTree()
                        bplustree_dict[type_name] = bptree
                        
                        for i in range(5, factor, 2):
                            field_names.append(tokenized[i])
                            field_types.append(tokenized[i+1])
                        with open("file_index.json", "r") as jsonFile:
                            data = json.load(jsonFile)
                        index = int(data["current_index"])
                        type_file_name = type_name + "-" + str(index) + ".txt"
                        status = create_type_file(type_name, primary_key_order, field_names, field_types, type_file_name, index)
                        type_data["types"].append(type_name)
                        type_data[type_name]=num_of_fields
                        with open("types.json", "w") as jsonFile:
                            json.dump(type_data, jsonFile)
                        with open("prim_keys.json", "r") as jsonFile:
                            prim_data = json.load(jsonFile)
                        prim_data[type_name]=primary_key_order
                        with open("prim_keys.json", "w") as jsonFile:
                            json.dump(prim_data, jsonFile)
                        logger(logFile, command.strip(), status)
                        data["current_index"] = index + 1
                        with open("file_index.json", "w") as jsonFile:
                            json.dump(data, jsonFile)
                else:
                    #LOG FAILURE
                    #Invalid create command
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                #Invalid create command
                logger(logFile, command.strip(), False)

        elif data == "record":
            #CREATE RECORD
            #Check the command
            if len(tokenized) > 2:
                #Check if a type with the given type name exists
                type_name = tokenized[2]
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                    #Check if the appropriate number of fields is given
                    if len(tokenized) == (3 + type_data[type_name]):

                        """
                        1- Check leaf nodes of the B+ tree of the type
                        2- If there is an entry with the same primary key in a leaf node, log failure and move on to the next command (continue)
                        3- If not, continue as written below
                        """
                        bptree = bplustree_dict[type_name]
                        with open("prim_keys.json", "r") as jsonFile:
                            prim_data = json.load(jsonFile)
                        order = prim_data[type_name]-1
                        prim_key = tokenized[3+order]
                        res = bptree.query(prim_key)
                        temp_f_name = type_name + "-"
                        if res is None:
                            files = []
                            for file in os.listdir(os.getcwd()):
                                if file.startswith(temp_f_name):
                                    if not file.endswith("-tree.json"):
                                        files.append(str(file))
                            #Sort the file list according to their names ascending order
                            files.sort(key=get_file_index)
                            #Locate the target file that is the first file, which has an empty available space for the record
                            target_file = None
                            exp_header = get_file_header(files[0])
                            for file in files:
                                is_full = is_file_full(file)
                                if not is_full:
                                    target_file = file
                                    break
                            #If there is not any file with empty available space, create one
                            if target_file is None:
                                with open("file_index.json", "r") as jsonFile:
                                    data = json.load(jsonFile)
                                index = int(data["current_index"])
                                type_file_name = type_name + "-" + str(index) + ".txt"
                                create_additional_file(index, exp_header["type"], exp_header["primary_key"], exp_header["fields"], exp_header["field_types"], type_file_name)
                                target_file = type_file_name
                                data["current_index"] = index + 1
                                with open("file_index.json", "w") as jsonFile:
                                    json.dump(data, jsonFile)
                            #Check if the field types are matching with the target file
                            candidate_fields = []
                            for i in range(3, len(tokenized)):
                                candidate_fields.append(tokenized[i])
                            target_field_types = get_file_field_types(target_file)
                            for i in range(len(candidate_fields)):
                                if target_field_types[i] == "str":
                                    #String type field
                                    try:
                                        str(candidate_fields[i])
                                    except:
                                        #LOG FAILURE
                                        #Type error
                                        logger(logFile, command.strip(), False)
                                elif target_field_types[i] == "int":
                                    #Integer type field
                                    try:
                                        int(candidate_fields[i])
                                    except:
                                        #LOG FAILURE
                                        #Type error
                                        logger(logFile, command.strip(), False)
                                else:
                                    #LOG FAILURE
                                    #Type error
                                    logger(logFile, command.strip(), False)
                            #All fields are given in appropriate type
                            result = add_record(target_file, candidate_fields)
                            status = result[0]
                            record_index = result[1]
                            """
                            1- Get the appropriate B+ tree from the B+ tree dict
                            2- Insert the record's primary key to that tree
                            3- Update the dict by the given key and its value by the updated tree
                            """
                            bptree.insert(prim_key, record_index)
                            bplustree_dict[type_name] = bptree
                            #LOG SUCCESS
                            logger(logFile, command.strip(), status)
                        else:
                            #LOG FAILURE
                            #Trying to create a record with a duplicate primary key
                            logger(logFile, command.strip(), False)
                    else:
                        #LOG FAILURE
                        #Trying to create a record with inappropriate field specification
                        logger(logFile, command.strip(), False)
                else:
                    #LOG FAILURE
                    #Trying to create a record of a non-existing type
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                logger(logFile, command.strip(), False)
        else:
            #LOG FAILURE
            logger(logFile, command.strip(), False)
    elif operation == "delete":
        if data == "type":
            #DELETE TYPE
            #Check if the command is valid
            if len(tokenized) == 3:
                #Check if the given type exists
                type_name = tokenized[2]
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                    #Get all files of the type
                    files = []
                    temp_f_name = type_name + "-"
                    for file in os.listdir(os.getcwd()):
                        if file.startswith(temp_f_name):
                            files.append(str(file))
                    #Delete all the files of the type
                    for file in files:
                        if os.path.exists(file):
                            os.remove(file)
                        else:
                            #LOG FAILURE
                            logger(logFile, command.strip(), False)
                            break
                    #Update the types.json file according to the deletion operation
                    type_data["types"].remove(type_name)
                    del type_data[type_name]
                    with open("types.json", "w") as jsonFile:
                        json.dump(type_data, jsonFile)
                    #Update the prim_keys.json file according to the deletion operation
                    with open("prim_keys.json", "r") as jsonFile:
                        prim_data = json.load(jsonFile)
                    del prim_data[type_name]
                    with open("prim_keys.json", "w") as jsonFile:
                        json.dump(prim_data, jsonFile)
                    """
                    1- Locate the B+ tree object of the given type from the B+ tree dict
                    2- Remove the object from the global B+ tree dict using the key 
                    3- Remove its file from the current directory
                    """
                    del bplustree_dict[type_name]
                    tree_file = type_name + "-tree.json"
                    if os.path.exists(tree_file):
                        os.remove(tree_file)
                    #LOG SUCCESS
                    logger(logFile, command.strip(), True)
                else:
                    #LOG FAILURE
                    #Trying to delete a type that is not created yet
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)

        elif data == "record":
            #DELETE RECORD
            if len(tokenized) == 4:
                type_name = tokenized[2]
                #Check if the type exists
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                    key = tokenized[3]
                    """
                    1- Use type_name to get the tree from the dict
                    2- Get the record index of the record from the B+ tree in the dict
                    3- Returns None if the key does not exist
                    """
                    bptree = bplustree_dict[type_name]
                    record_index = bptree.query(key)
                    if record_index is None:
                        #LOG FAILURE
                        #Trying to delete a non-existing record
                        logger(logFile, command.strip(), False)
                    else:
                        #Record found, its index is stored in record_index
                        file_name = type_name + "-" + str(record_index[0]) + ".txt"
                        status = delete_record(file_name, record_index, key)
                        """
                        1- if status is True, then record is deleted from file
                        2- delete record from the B+ Tree found in the B+ Tree dict with the type_name as the key
                        """
                        if status:
                            bptree.delete(key)
                            bplustree_dict[type_name] = bptree
                        #Logs success if status is True, failure if False
                        logger(logFile, command.strip(), status)
                else:
                    #LOG FAILURE
                    #Trying to delete a record of a non-existing type
                    logger(logFile, command.strip(), False)
                pass
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)
        else:
            #LOG FAILURE
            logger(logFile, command.strip(), False)
            
    elif operation == "list":
        if data == "type":
            #LIST TYPE
            if len(tokenized) == 2:
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if types:
                    for type in types:
                        output_file.write(type + "\n")
                    logger(logFile, command.strip(), True) 
                else:
                    #No type exists
                    logger(logFile, command.strip(), False)        
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)
            pass
        elif data == "record":
            #LIST RECORD
            if len(tokenized) == 3:
                type_name = tokenized[2]
                #Check if the type exists
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                #Get all files of the type
                    files = []
                    for file in os.listdir(os.getcwd()):
                        if file.startswith(type_name):
                            if not file.endswith("-tree.json"):
                                files.append(str(file))
                    for file in files:
                        outputs = get_all_records(file)
                        if len(outputs):
                            for output in outputs:
                                output_file.write(output + "\n")
                        else:
                            #EMPTY FILE
                            pass
                    logger(logFile, command.strip(), True)
                else:
                    #LOG FAILURE
                    #Trying to list the records of a nonexisting type
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)
        else:
            #LOG FAILURE
            pass
    elif operation == "update":
        if data == "record":
            #UPDATE RECORD
            if len(tokenized) > 3:
                type_name = tokenized[2]
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                    num_of_fields = type_data[type_name]
                    if len(tokenized) == (3+num_of_fields):
                        primary_key = tokenized[3]
                        bptree = bplustree_dict[type_name]
                        record_index = bptree.query(primary_key)
                        if record_index is None:
                            #LOG FAILURE
                            #Non-existing record with the primary key
                            logger(logFile, command.strip(), False)
                        else:
                            #Record found, its index is stored in record_index
                            file_name = type_name + "-" + str(record_index[0]) + ".txt"
                            fields = tokenized[3:]
                            status = update_record(file_name, record_index, fields)
                            logger(logFile, command.strip(), status)
                    else:
                        #LOG FAILURE
                        #Invalid command
                        logger(logFile, command.strip(), False)
                else:
                    #LOG FAILURE
                    #Trying to update a record of a non-existing type
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)
        else:
            #LOG FAILURE
            pass
    elif operation == "search":
        if data == "record":
            #SEARCH RECORD
            if len(tokenized) == 4:
                type_name = tokenized[2]
                primary_key = tokenized[3]
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                    #Get b+ tree from dict using type_name
                    bptree = bplustree_dict[type_name]
                    record_index = bptree.query(primary_key)
                    if record_index is None:
                        #LOG FAILURE
                        #No results for the given primary key
                        logger(logFile, command.strip(), False)
                    else:
                        #Record found, its index is stored in record_index
                        file_name = type_name + "-" + str(record_index[0]) + ".txt"
                        result = search_record(file_name, record_index)
                        output_file.write(result + "\n")
                        logger(logFile, command.strip(), True)
                else:
                    #LOG FAILURE
                    #Trying to search for record of a non-existing type
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)
        else:
            #LOG FAILURE
            pass
    elif operation == "filter":
        if data == "record":
            #FILTER RECORD
            if len(tokenized) == 4:
                type_name = tokenized[2]
                with open("types.json", "r") as jsonFile:
                    type_data = json.load(jsonFile)
                types = type_data["types"]
                if type_name in types:
                    condition = tokenized[3]
                    if ">" in condition:
                        #Greater than
                        cond_data = condition.split('>')
                        if len(cond_data) == 2:
                            primary_key = cond_data[0]
                            parameter = cond_data[1]
                            operation = ">"
                            #TODO get the b+tree of type
                            bptree = bplustree_dict[type_name]
                            result_indexes = bplustree.range(bptree, operation, parameter)
                            if result_indexes is None:
                                #LOG FAILURE
                                #No result
                                logger(logFile, command.strip(), False)
                            else:
                                #Successfully filtered
                                file_names = []
                                for item in result_indexes:
                                    file_index = item[0]
                                    cond = "-" + str(file_index) + ".txt"
                                    for file in os.listdir(os.getcwd()):
                                        if file.endswith(cond):
                                            file_names.append(str(file))
                                outputs = filter_record(file_names, result_indexes)
                                for item in outputs:
                                    output_file.write(item + "\n")
                                logger(logFile, command.strip(), True)
                        else:
                            #LOG FAILURE
                            #Invalid command
                            logger(logFile, command.strip(), False)
                    elif "<" in condition:
                        #Smaller than
                        cond_data = condition.split('<')
                        if len(cond_data) == 2:
                            primary_key = cond_data[0]
                            parameter = cond_data[1]
                            operation = "<"
                            bptree = bplustree_dict[type_name]
                            result_indexes = bplustree.range(bptree, operation, parameter)
                            if result_indexes is None:
                                #LOG FAILURE
                                #No result
                                logger(logFile, command.strip(), False)
                            else:
                                #Successfully filtered
                                file_names = []
                                for item in result_indexes:
                                    file_index = item[0]
                                    cond = "-" + str(file_index) + ".txt"
                                    for file in os.listdir(os.getcwd()):
                                        if file.endswith(cond):
                                            file_names.append(str(file))
                                outputs = filter_record(file_names, result_indexes)
                                for item in outputs:
                                    output_file.write(item + "\n")
                                logger(logFile, command.strip(), True)
                        else:
                            #LOG FAILURE
                            #Invalid command
                            logger(logFile, command.strip(), False)
                        pass
                    elif "=" in condition:
                        #Equality
                        cond_data = condition.split('=')
                        if len(cond_data) == 2:
                            primary_key = cond_data[0]
                            parameter = cond_data[1]
                            operation = "="
                            bptree = bplustree_dict[type_name]
                            result_indexes = bplustree.range(bptree, operation, parameter)
                            if result_indexes is None:
                                #LOG FAILURE
                                #No result
                                logger(logFile, command.strip(), False)
                            else:
                                #Successfully filtered
                                file_names = []
                                for item in result_indexes:
                                    file_index = item[0]
                                    cond = "-" + str(file_index) + ".txt"
                                    for file in os.listdir(os.getcwd()):
                                        if file.endswith(cond):
                                            file_names.append(str(file))
                                outputs = filter_record(file_names, result_indexes)
                                for item in outputs:
                                    output_file.write(item + "\n")
                                logger(logFile, command.strip(), True)
                        else:
                            #LOG FAILURE
                            #Invalid command
                            logger(logFile, command.strip(), False)
                        pass
                    else:
                        #LOG FAILURE
                        #Invalid command
                        logger(logFile, command.strip(), False)
                else:
                    #LOG FAILURE
                    #Trying to filter a record of a non-existing type
                    logger(logFile, command.strip(), False)
            else:
                #LOG FAILURE
                #Invalid command
                logger(logFile, command.strip(), False)
        else:
            #LOG FAILURE
            #Invalid command
            logger(logFile, command.strip(), False)
    else:
        #LOG FAILURE
        #Invalid command
        logger(logFile, command.strip(), False)

"""
1- Iterate through the global B+ tree dict
2- Save each B+ Tree object in that dict to a file in the current directory using pickle with appropriate file name
"""
for type in bplustree_dict:
    tree = bplustree_dict[type]
    tree_dict = bplustree.tree_serializer(tree)
    file_name = type + "-tree.json"
    with open(file_name, "w") as jsonFile:
        json.dump(tree_dict, jsonFile)