# set up python program to sort files
import re
import shutil
import os

def main(my_file_path):
    print("stand by:")
    tmp_file_path = './tempFile.txt'

    makeTempFile(my_file_path)
    getNote(tmp_file_path)
    getWarning(tmp_file_path)
    getUH60L(tmp_file_path)
    getUH60M(tmp_file_path)

# Copy the original file to preserve forensic data
def makeTempFile(my_file_path):
    shutil.copy(my_file_path, './tempFile.txt')
    

# Check for UH-60M
def getUH60M(tmp_file_path):
    # Check for existance of folder
    if not os.path.exists("./UH-60M/"):
        os.mkdir("./UH-60M/")
        print("Directory ./UH-60M/ created")
    else:
        print("Directory ./UH-60M/ already exists")
    # Scan file for matches
    with open(tmp_file_path, 'r+') as f:
        UHM=open("./UH-60M/UH-60M.txt", "a")
        for x in f:
            for matchedText in x.split():
                if ".UH-60M." in matchedText:
                    # if a match is found, write to the file
                    print(x,end='')
                    UHM.write(x)
    UHM.close()
    f.close()

# Check for UH-60L
def getUH60L(tmp_file_path):    
    if not os.path.exists("./UH-60L/"):
        os.mkdir("./UH-60L/")
        print("Directory ./UH-60L/ created")
    else:
        print("Directory ./UH-60L/ already exists")    
    with open(tmp_file_path, 'r+') as f:
        UHL=open("./UH-60L/UH-60L.txt","a")
        for x in f:
            for matchedText in x.split():
                if ".UH-60L." in matchedText:
                    print(x)
                    UHL.write(x)
    UHL.close()
    f.close()

# Check for UH-60AL
def getUH60L(tmp_file_path): 
    if not os.path.exists("./UH-60AL/"):
        os.mkdir("./UH-60AL/")
        print("Directory ./UH-60AL/ created")
    else:
        print("Directory ./UH-60AL/ already exists")      
    with open(tmp_file_path, 'r+') as f:
        UHAL=open("./UH-60AL/UH-60AL.txt","a")
        for x in f:
            for matchedText in x.split():
                if ".UH-60A." in matchedText:
                    print(x)
                    UHAL.write(x)
    UHAL.close()
    f.close()

# Check for Notes
def getNote(tmp_file_path):   
    if not os.path.exists("./NOTES/"):
        os.mkdir("./NOTES/")
        print("Directory ./NOTES/ created")
    else:
        print("Directory ./NOTES/ already exists") 
    with open(tmp_file_path, 'r') as f:
        nl=open("./NOTES/noteList.txt","a")           
        for x in f:
            for matchedText in x.split():
                if "NOTE" in matchedText:
                    print(x)
                    nl.write(x)
                   
    nl.close()
    f.close() 

# Check for Warnings
def getWarning(tmp_file_path):
    with open(tmp_file_path, 'r') as f:
        wa=open("warningList.txt","a")           
        for x in f:
            for matchedText in x.split():
                if "WARNING" in matchedText:
                    print(x)
                    wa.write(x)
    wa.close()
    f.close()

# Entry Point
if __name__ == "__main__":
    my_file_path = "./RDF_Database.log_201907_2019_Q2_H-60.txt" # this url will change
    main(my_file_path)
