
'''[summary]
This file will handle converting gz archives to xz archives in convenient fashion.
'''
import argparse
import datetime
import gzip
import logging
import lzma
import os
import re
import shutil
import time
import zipfile
from multiprocessing import Process, cpu_count#, queue
import pymongo # This has been added to save to log files in a database
from collections import deque


# Set up MongoDB
client = pymongo.MongoClient()
clientName = "gz_to_xz_log_records"
collectionName = "gz_to_xz"
timeNow = datetime.datetime.now()

db = client[ clientName ] # makes a test database called "testdb"
col = db[ collectionName ] #makes a collection called "testcol" in the "testdb"

def db_logging_function(data):
    myTime = datetime.datetime.now()
    col.insert_one({"timestamp": myTime,"data":data})

# Allowable logging keywords:
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected. This is the standard
# WARNING: Possible issue but the software is still working as expected.
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A serious error, indicating that the program itself may be unable to continue running.

# logging.basicConfig(filename='gz_to_xz_log.log', level=logging.DEBUG)
logging.basicConfig(filename='gz_to_xz_log.log', level=logging.INFO)

# Handle gz
def gz_xz(fn: str, decomp_check=True, profiling=True, keep=False):
    #print("gz_xz") #debugging
    logging.debug("Just entered gz_xz()")
    logging.debug("Arguments:\nfn: {}\ndecomp_check: {}\nprofiling: {}\nkeep: {}\n".format(fn, decomp_check, profiling, keep))
    
    # check if valid
    try:
        assert os.path.exists(fn), f'File: {fn} doesn\'t exist'
        assert fn.lower().endswith('.gz'), f'File: {fn} is not a gzip file(not a gz ext)'

        # extract
        i_st = os.stat(fn)
        print("i_st: {}".format(i_st))
        start = time.time()
        with gzip.open(fn) as fgz:
            uncompressed = fgz.read()
            ofn = fn[:-2] + 'xz'
            fgz.close()
            #decomp_check = False # debugging
            make_xz(fn, ofn, uncompressed, i_st, decomp_check, keep)
        
        # Set start time
        data = "{} process time: {}".format(fn, time.time() - start)
        print(data)
        logging.info(data)
        db_logging_function(data)

    except Exception as e:
        data = "{}\nFile {} caused a problem.".format(e.args, fn)
        logging.error(data)
        bad_file_list(fn)
        db_logging_function(data)

# Handle zip
def zip_xz(file_name: str, decomp_check=True, profiling=True, keep=False):
    # Removed old commeted out code - improved readability
    logging.debug("Just entered zip_xz()")
    #print("zip_xz") #debugging
    # check if valid
    assert os.path.exists(file_name), f'File: {file_name} doesn\'t exist'
    assert file_name.lower().endswith('.zip'), f'File: {file_name} is not a zip file(not a zip ext)'
    assert zipfile.is_zipfile(file_name), f'File: {file_name} is not a VALID zip file(fails zip file header check)'
    # extract
    i_st = os.stat(file_name)

    start = time.time()
    with zipfile.ZipFile(file_name) as fzip:
        ofn = file_name[:-3] + 'xz'
        print("ofn: {ofn}\nfn[:-3]: {nfn[:-3]}") # debugging
        rdf_fn = file_name[:-4]
        assert '.rdf' in rdf_fn.lower(), f'File: {file_name} is not a rdf file(not a rdf ext)'
        assert len(fzip.namelist()) == 1, f'{file_name} contains multiple files.  This is not expected, terminating process.'
        just_fn = os.path.basename(rdf_fn)
        uncompressed = fzip.open(just_fn).read()
        fzip.close()
        make_xz(file_name, ofn, uncompressed, i_st, decomp_check, keep)

    data = "{} process time:".format(file_name, {time.time() - start})
    print(data)
    logging.info(data)
    db_logging_function(data)


def file_to_xz(file_name: str, decomp_check=True, profiling=True, keep=False):
    #print("file_to_xz") #debugging
    logging.debug("Just entered file_to_xz()")

    try:
        #print(0/0) # debugging - intentional error: division by zero
        _, file_extension = os.path.splitext(file_name)
        # print(file_extension)
        logging.debug("corrupt = False\nfile_extension = {}".format(file_extension))
        if file_extension.lower().endswith('gz'):
            gz_xz(file_name, decomp_check, profiling, keep)
            logging.debug("File was gz\nfn = {}\ndecomp_check = {}\nprofiling = {}\nkeep = {}".format(file_name, decomp_check, profiling, keep))
        if file_extension.lower().endswith('zip'):
            zip_xz(file_name, decomp_check, profiling, keep)
            logging.debug("File was zip\nfn = {}\ndecomp_check = {}\nprofiling = {}\nkeep = {}".format(file_name, decomp_check, profiling, keep))
        # Removed commented out code - improved readability
    except Exception as e:
        data = "Error!\n{}".format(e.args)
        print(data)
        logging.error(data)
        bad_file_list(file_name)
        db_logging_function(data)


def make_xz(file_name: str, output_file_name: str, uncompressed, current_status, decomp_check=True, keep=False):
    try:
        #print("make_xz") #debugging
        with lzma.open(output_file_name, mode='wb') as fxz:
            fxz.write(uncompressed)
        # copy permissions, modified, creation times ext to maintain records
        os.chmod(output_file_name, 0o777)
        # get input file ownership
        if os.name == 'posix':
            shutil.chown(ofn, current_status.st_uid, current_status.st_gid)
        os.utime(output_file_name, (current_status.st_atime, current_status.st_mtime))
        if decomp_check:
            with lzma.open(output_file_name, mode='rb') as fxz:
                decompressed = fxz.read()
                assert decompressed == uncompressed
                if not keep:
                    os.remove(file_name)
    except Exception as e:
        data = "Error in make_xz():\n{}\n".format(e.args)
        logging.error(data)
        bad_file_list(file_name)
        db_logging_function(data)

def get_file_list(starting_pathway, file_ext_list = ['gz', 'zip']):
    #print("get_fl") #debugging
    logging.debug("Entered get_file_list()")
    result = []
    file_exts = "("+"|".join(file_ext_list) + ")"
    #print("\nfile_exts: {}\n".format(file_exts)) # debugging
    my_regex = re.compile(r"(?i).*RDF.*." + file_exts)
    
    try:
        for root, _, files in os.walk(starting_pathway):
            try:
                for file in files:
                    print("file: {}\n".format(file)) # debugging
                    # if file.endswith(".gz"):
                    if my_regex.match(file):
                        file_path = os.path.join(root, file)
                        result.append(file_path)
                        data = "\nFound match: {}".format(file_path)
                        print(data)
                        db_logging_function(data)
                        logging.debug("REGEX r1 caught: {}".format(file_path))
                    elif file.endswith('.gz'):
                        file_path = os.path.join(root, file)
                        result.append(file_path)
                        data = "\nFound match (endswith): {}".format(file_path)
                        print(data)
                        db_logging_function(data)
                        logging.debug("file.endswith() caught: {}".format(file_path))

            except Exception as e:
                data = "get_fl inner loop failure: {}\nPath: {}".format(e.args, file_path)
                print(data)
                logging.error(data)
                bad_file_list(file)
                db_logging_function(data)

    except Exception as e:
        data = "\nI failed in get_fl outer loop\n{}".format(e.args)
        print(data)
        logging.error(data)
        bad_file_list(file)
        db_logging_function(data)
    finally:
        logging.debug("Just hit finally in get_fl() try-except block.")
        return result
# https://everydayimlearning.blogspot.com/2013/03/multiprocessing-with-python.html

def fileListProcessing(files, queue_of_files):
    logging.debug("Entered fileListProcessing()")
    '''
    Puts first lines from all listed files into a queue. Provides a safe way of getting the result from several processes.
    '''
    #print("fileListProcessing") #debugging
    result = []
    for filename in files:
        logging.debug("Entered loop: for filename in files:\nCall to file_to_xz(filename)")
        file_to_xz(filename)
    logging.debug("result = {}".format(result))


# And here is an actual multiprocessing:

def myMultiprocessing(folder= None,file_list=None, extensions=['gz','zip'], threads = None):
    '''
    Splits the source filelist into sublists according to the number of CPU cores and provides multiprocessing of them.
    '''
    logging.debug("[Step 2] Just entered myMultiprocessing()") # debugging
    #print("myMultiprocessing") #debugging
    
    logging.debug("[Step 2a] if threads is None") # debugging
    if threads is None:
        threads = cpu_count()
        #print("<threads> == None") # debugging
        logging.debug("[Step 2a1] threads = {}".format(threads))  # debugging

    logging.debug("[Step 2b] if folder is None and file_list is not None:")  # debugging
    if folder is None and file_list is not None:
        raw_file = open(file_list, mode='r').read()
        files = raw_file.splitlines()
        logging.debug("[Step 2b1] files = {}".format(files))  # debugging
        #print("<folder> == None <file_list> != None") # debugging

    logging.debug("[Step 2c] if folder is not None:")
    if folder is not None:
        try:
            files = get_file_list(folder, extensions)
            #print("<folder> != None\nfolder: {}\nexts: {}\n".format(folder,exts)) # debugging
            logging.debug("[Step 2c1] files = {}".format(files))  # debugging
        except Exception as e:
            data = "I failed in myMultiprocessing!\nError: {}\nFolder: {}\nexts: {}\n".format(e.args, folder, extensions)
            print(data)
            logging.error(data)
            db_logging_function(data)
    else:
        assert folder is not None
        #print("<folder> == {}\n<files> == {}".format(folder, files)) # debugging
        logging.debug("[Step 2c Else] assert folder is not None.") # debugging
    queue_of_files = deque()
    list_of_thread_procs = []
    #print("q = queue(): {}".format(q)) # debugging
    data = "Number of threads: {}".format(threads)
    print(data)
    db_logging_function(data)
    data = "{} files found.".format(len(files))
    print(data)
    db_logging_function(data)

    logging.debug("[Step 3] for i in range(threads):")  # debugging
    for i in range(threads):
        # Split the source filelist into several sublists.
        
        list_of_files = [files[j] for j in range(len(files)) if j % threads == i]
        logging.debug("[Step 3a] Set list_of_files variable: {}".format(list_of_files))  # debugging
        #print("list_of_files: {}".format(list_of_files)) # debugging
        logging.debug("[Step 3b] if len(list_of_files) > 0:")  # debugging
        if len(list_of_files) > 0:
            thread_process = Process(target=fileListProcessing, args=([list_of_files, queue_of_files]))
            #print("p: {}".format(p)) # debugging
            logging.debug("[Step 3b1] p = {}".format(thread_process))  # debugging
            thread_process.start()
            list_of_thread_procs += [thread_process]
            #print("procs: {}".format(procs)) # debugging
            logging.debug("[Step 3b2] procs = {}".format(list_of_thread_procs))  # debugging
    # Collect the results:
    all_results = []
    for i in range(len(list_of_thread_procs)):
        # Save all results from the queue.
        all_results += queue_of_files.get()
        
    return len(files)

def main(input_directory=None, file_list=None, verify=True, threads=None, exts=['gz','zip']):
    try:
        #print("main") #debugging
        logging.debug("[Step 1] Just entered main()")  # track progress
        # TODO: add parallelism for this as a cli parameter
        assert (input_directory is None or os.path.exists(input_directory)) , f'Directory: {input_directory} does not exist'
        assert (file_list is None or  os.path.exists(file_list)), f'File list: {file_list} does not exist.'
        assert not (input_directory is None and file_list is None), 'Need a directory or file list'

        logging.debug("About to check number of threads in command line argument.") # track progress
        start = time.time()
        if threads is None:
            logging.debug("variable <threads> is None")
            logging.debug("Calling myMultiprocessing()")
            processed = myMultiprocessing(input_directory,file_list)

        else:
            logging.debug("variable <threads> is not None:")
            logging.debug("Calling myMultiprocessing")
            processed = myMultiprocessing(input_directory, file_list, threads=threads)

        logging.debug("Command line arguments parsed:\nidir: {}\nflist: {}\nthreads: {}".format(input_directory, file_list, threads))
        print(f"Files selected for processing:  {processed}")
        print(f"Total processing time:  {time.time()-start}")
        logging.info("Files selected for processing:  {}".format(processed))
        logging.info("Total processing time:  {}".format(time.time() - start))
    except Exception as e:
        logging.error("Error caught in main(): {}".format(e.args))
    finally:
        logging.debug("Reached main() finally block")

def bad_file_list(bad_file_name):
    add2file = open("error_files.txt", "a")
    entry_for_list="file: "+bad_file_name+"\n"
    add2file.write(entry_for_list)
    add2file.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser() # ArgumentParser allows the use of command line args

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-l', '--file_list', help="File list in lieu of directory")
    group.add_argument('-d', '--input_directory', help='Input directory to recursively reformat.')
    parser.add_argument("-v", "--verify", help="Decompression check performed", action="store_true")
    parser.add_argument("-t", "--threads", type=int, help="Number of threads")

    args = parser.parse_args()
    passed_arguments = "Parsed arguments: {}".format(args)
    print(passed_arguments)
    db_logging_function(passed_arguments)
    print_date = "\n\n**********  {} **********\n".format(datetime.datetime.now())
    print(print_date)
    logging.info(print_date)
    logging.debug('About to call main()') # track progress
    logging.info('args: {}'.format(args))
    db_logging_function(print_date)
    db_logging_function('args: {}'.format(args))
    main(**vars(args))
