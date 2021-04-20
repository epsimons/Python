#!/usr/bin/env python3
#
# Scan the local an sub folders
# create a list of all items found
#
# @author Ethan Simons
#

import os
import re


def findFiles(my_file_path):
    # take passed in file path
    #dirs = os.listdir(my_file_path)

    # create the text file

    txtFile = open("FileList2.txt","w+")


    for dirpath, dirnames, filenames in os.walk(my_file_path):
        dirnames = dirnames # This is to quell the pylint unsused var issue
        for f in filenames:
            fp = os.path.relpath(os.path.join(dirpath))

            # files in the same directory as the program
            # will show up with an extra "./"
            # this checks and removes that
            output = str("./"+fp+"/"+f)
            if output[:4]=="././":
                output=output[2:]
            print(output)

            txtFile.write(output+"\n")
    # close and save the text file
    txtFile.close()




# query the user for a folder path
#my_file_path = input("Enter folder path or leave blank for current folder:")

# check if the user left the field blank
#if not my_file_path == "":
    # if blank, set the working directory as the path

my_file_path = os.path.dirname(os.path.realpath(__file__))
# send the folderpath data to the method
findFiles(my_file_path)
