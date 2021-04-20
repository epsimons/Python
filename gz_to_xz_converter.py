
'''[summary]
This file will handle converting gz archives to xz archives in convenient fashion.
'''
import argparse
import lzma
import gzip
import os
import re
import zipfile
import time
import shutil
import logging
import datetime
import subprocess

# Allowable logging keywords:
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected. This is the standard
# WARNING: Possible issue but the software is still working as expected.
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A serious error, indicating that the program itself may be unable to continue running.

# logging.basicConfig(filename='gz_to_xz_log.log', level=logging.DEBUG)
logging.basicConfig(filename='gz_to_xz_log.log', level=logging.INFO)

# TODO: Establish global variable to hold a list of files that cannot be
# processed for one reason or another



# Handle gz
def gz_xz(fn: str, decomp_check=True, profiling=True, keep=False):
    #print("gz_xz") #debugging
    logging.debug("Just entered gz_xz()")
    logging.debug("Arguments:\nfn: {}\ndecomp_check: {}\nprofiling: {}\nkeep: {}\n".format(fn, decomp_check, profiling, keep))
    '''Converts a gz file to an xz file.

    Arguments:
        fn {str} -- Input file name

    Keyword Arguments:
        decomp_check {bool} -- Flag for uncompressing file and checking buffer. (default: {True})
        profiling {bool} -- Flag for reporting profiling information (default: {True})
    '''
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
        print(f'{fn} process time: {time.time() - start}')
        logging.info("{} process time: {}".format(fn, time.time() - start))
    except Exception as e:
        logging.error("{}\nFile {} caused a problem.".format(e.args, fn))
        bad_file_list(fn)

# Handle zip
def zip_xz(fn: str, decomp_check=True, profiling=True, keep=False):
    '''Converts a zip file to an xz file.

    Arguments:
        fn {str} -- Input file name

    Keyword Arguments:
        decomp_check {bool} -- Flag for uncompressing file and checking buffer. (default: {True})
        profiling {bool} -- Flag for reporting profiling information (default: {True})
    '''
    logging.debug("Just entered zip_xz()")
    #print("zip_xz") #debugging
    # check if valid
    assert os.path.exists(fn), f'File: {fn} doesn\'t exist'
    assert fn.lower().endswith('.zip'), f'File: {fn} is not a zip file(not a zip ext)'
    assert zipfile.is_zipfile(fn), f'File: {fn} is not a VALID zip file(fails zip file header check)'
    # extract
    i_st = os.stat(fn)

    start = time.time()
    with zipfile.ZipFile(fn) as fzip:
        ofn = fn[:-3] + 'xz'
        print("ofn: {ofn}\nfn[:-3]: {nfn[:-3]}") # debugging
        rdf_fn = fn[:-4]
        assert '.rdf' in rdf_fn.lower(), f'File: {fn} is not a rdf file(not a rdf ext)'
        assert len(fzip.namelist()) == 1, f'{fn} contains multiple files.  This is not expected, terminating process.'
        just_fn = os.path.basename(rdf_fn)
        uncompressed = fzip.open(just_fn).read()
        fzip.close()
        make_xz(fn, ofn, uncompressed, i_st, decomp_check, keep)

    print(f'{fn} process time: {time.time() - start}')
    logging.info("{} process time: {}".format(fn, time.time() - start))


def file_to_xz(fn: str, decomp_check=True, profiling=True, keep=False):
    #print("file_to_xz") #debugging
    logging.debug("Just entered file_to_xz()")

    try:
        #print(0/0) # debugging - intentional error: division by zero
        _, file_extension = os.path.splitext(fn)
        # print(file_extension)
        logging.debug("corrupt = False\nfile_extension = {}".format(file_extension))
        if file_extension.lower().endswith('gz'):
            gz_xz(fn, decomp_check, profiling, keep)
            logging.debug("File was gz\nfn = {}\ndecomp_check = {}\nprofiling = {}\nkeep = {}".format(fn, decomp_check, profiling, keep))
        if file_extension.lower().endswith('zip'):
            zip_xz(fn, decomp_check, profiling, keep)
            logging.debug("File was zip\nfn = {}\ndecomp_check = {}\nprofiling = {}\nkeep = {}".format(fn, decomp_check, profiling, keep))
        # This else statement did not seem to do anything useful.
        #else:
            #print("File: {} is not gz or zip".format(fn))
            #logging.warning("*** File {} is not gz or zip ***".format(fn))
    except Exception as e:
        print("Error!\n{}".format(e.args))
        logging.error("Error!\n{}".format(e.args))
        bad_file_list(fn)


def make_xz(fn: str, ofn: str, uncompressed, i_st, decomp_check=True, keep=False):
    try:
        #print("make_xz") #debugging
        with lzma.open(ofn, mode='wb') as fxz:
            fxz.write(uncompressed)
        # copy permissions, modified, creation times ext to maintain records
        os.chmod(ofn, 0o777)
        # get input file ownership
        if os.name == 'posix':
            shutil.chown(ofn, i_st.st_uid, i_st.st_gid)
        os.utime(ofn, (i_st.st_atime, i_st.st_mtime))
        if decomp_check:
            with lzma.open(ofn, mode='rb') as fxz:
                decompressed = fxz.read()
                assert decompressed == uncompressed
                if not keep:
                    os.remove(fn)
    except Exception as e:
        logging.error("Error in make_xz():\n{}\n".format(e.args))
        bad_file_list(fn)
# def walk_convert(cdir):
#     r1 = re.compile(r"(?i).*RDF.*.(gz|zip)")
#     # ignoring dirs (middle tuple val)
#     for root, _, files in os.walk(cdir):
#         for file in files:
#             # if file.endswith(".gz"):
#             if r1.match(file):
#                 fpath = os.path.join(root, file)
#                 gz_xz(fpath)

def get_fl(cdir, file_ext_list = ['gz', 'zip']):
    #print("get_fl") #debugging
    logging.debug("Entered get_fl()")
    result = []
    file_exts = "("+"|".join(file_ext_list) + ")"
    #print("\nfile_exts: {}\n".format(file_exts)) # debugging
    r1 = re.compile(r"(?i).*RDF.*." + file_exts)
    #
    # BUG: regex will not catch all
    #
    # ignoring dirs (middle tuple val)
    try:
        for root, _, files in os.walk(cdir):
            try:
                for file in files:
                    print("file: {}\n".format(file)) # debugging
                    # if file.endswith(".gz"):
                    if r1.match(file):
                        fpath = os.path.join(root, file)
                        result.append(fpath)
                        print("\nFound match: {}".format(fpath))
                        logging.debug("REGEX r1 caught: {}".format(fpath))
                    elif file.endswith('.gz'):
                        fpath = os.path.join(root, file)
                        result.append(fpath)
                        print("\nFound match (endswith): {}".format(fpath))
                        logging.debug("file.endswith() caught: {}".format(fpath))

            except Exception as e:
                print("get_fl inner loop failure: {}\nPath: {}".format(e.args, fpath))
                logging.error("get_fl inner loop failure: {}\nPath: {}".format(e.args, fpath))
                bad_file_list(file)
    except Exception as e:
        print("\nI failed in get_fl outer loop\n{}".format(e.args))
        logging.error("\nI failed in get_fl outer loop\n{}".format(e.args))
        bad_file_list(file)
    finally:
        logging.debug("Just hit finally in get_fl() try-except block.")
        return result
# https://everydayimlearning.blogspot.com/2013/03/multiprocessing-with-python.html

def fileListProcessing(files, q):
    logging.debug("Entered fileListProcessing()")
    '''
    Puts first lines from all listed files into a Queue. Provides a safe way of getting the result from several processes.
    '''
    #print("fileListProcessing") #debugging
    result = []
    for filename in files:
        logging.debug("Entered loop: for filename in files:\nCall to file_to_xz(filename)")
        file_to_xz(filename)
    #q.put([])
    #raise
    q.put(result)
    logging.debug("q.put(result): result = {}".format(result))
# And here is an actual multiprocessing:


from multiprocessing import Queue, Process, cpu_count

def myMultiprocessing(folder= None,file_list=None, exts=['gz','zip'], threads = None):
    '''
    Splits the source filelist into sublists according to the number of CPU cores and provides multiprocessing of them.
    '''
    logging.debug("[Step 2] Just entered myMultiprocessing()") # debugging
    #print("myMultiprocessing") #debugging
    # param management coudl be improved
    # TODO: See if something can be done here to aid in program continuity
    logging.debug("[Step 2a] if threads is None") # debugging
    if threads is None:
        threads = cpu_count()
        #print("<threads> == None") # debugging
        logging.debug("[Step 2a1] threads = {}".format(threads))  # debugging
    logging.debug("[Step 2b] if folder is None and file_list is not None:")  # debugging
    if folder is None and file_list is not None:
        f_raw = open(file_list, mode='r').read()
        files = f_raw.splitlines()
        logging.debug("[Step 2b1] files = {}".format(files))  # debugging
        #print("<folder> == None <file_list> != None") # debugging
        # files.pop()
        # print(files)
    logging.debug("[Step 2c] if folder is not None:")
    if folder is not None:
        try:
            files = get_fl(folder, exts)
            #print("<folder> != None\nfolder: {}\nexts: {}\n".format(folder,exts)) # debugging
            logging.debug("[Step 2c1] files = {}".format(files))  # debugging
        except Exception as e:
            print("I failed in myMultiprocessing!\nError: {}\nFolder: {}\nexts: {}\n".format(e.args, folder, exts))
            logging.error("I failed in myMultiprocessing!\nError: {}\nFolder: {}\nexts: {}\n".format(e.args, folder, exts))
    else:
        assert folder is not None
        #print("<folder> == {}\n<files> == {}".format(folder, files)) # debugging
        logging.debug("[Step 2c Else] assert folder is not None.") # debugging
    q = Queue()
    procs = []
    #print("q = queue(): {}".format(q)) # debugging
    print(f'Number of threads: {threads}')
    print(len(files), 'files found.')

    logging.debug("[Step 3] for i in range(threads):")  # debugging
    for i in range(threads):
        # Split the source filelist into several sublists.
        # FIXME: <lst> is empty so possibly no new files will be looked for
        #
        lst = [files[j] for j in range(len(files)) if j % threads == i]
        logging.debug("[Step 3a] Set lst variable: {}".format(lst))  # debugging
        #print("lst: {}".format(lst)) # debugging
        logging.debug("[Step 3b] if len(lst) > 0:")  # debugging
        if len(lst) > 0:
            p = Process(target=fileListProcessing, args=([lst, q]))
            #print("p: {}".format(p)) # debugging
            logging.debug("[Step 3b1] p = {}".format(p))  # debugging
            p.start()
            procs += [p]
            #print("procs: {}".format(procs)) # debugging
            logging.debug("[Step 3b2] procs = {}".format(procs))  # debugging
    # Collect the results:
    all_results = []
    for i in range(len(procs)):
        # Save all results from the queue.
        all_results += q.get()
        #print("all_results: {}".format(all_results)) # debugging

    # # Output results into the file.
    # log = open("logfile.log", "w")
    # print >>log, all_results
    # log.close()
    return len(files)

def main(idir=None,flist=None, verify=True, threads=None,exts=['gz','zip']):
    try:
        #print("main") #debugging
        logging.debug("[Step 1] Just entered main()")  # track progress
        # TODO: add parallelism for this as a cli parameter
        assert (idir is None or os.path.exists(idir)) , f'Directory: {idir} doesn\'t exist'
        assert (flist is None or  os.path.exists(flist)), f'File list: {flist} does not exist.'
        assert not (idir is None and flist is None), 'Need a directory or file list'

        logging.debug("About to check number of threads in command line argument.") # track progress
        start = time.time()
        if threads is None:
            logging.debug("variable <threads> is None")
            logging.debug("Calling myMultiprocessing()")
            processed = myMultiprocessing(idir,flist)

        else:
            logging.debug("variable <threads> is not None:")
            logging.debug("Calling myMultiprocessing")
            processed = myMultiprocessing(idir, flist, threads=threads)

        logging.debug("Command line arguments parsed:\nidir: {}\nflist: {}\nthreads: {}".format(idir, flist, threads))
        print(f"Files selected for processing:  {processed}")
        print(f"Total processing time:  {time.time()-start}")
        logging.info("Files selected for processing:  {}".format(processed))
        logging.info("Total processing time:  {}".format(time.time() - start))
    except Exception as e:
        logging.error("Error caught in main(): {}".format(e.args))
    finally:
        logging.debug("Reached main() finally block")

def bad_file_list(bfname):
    add2file = open("error_files.txt", "a")
    mytext="file: "+bfname+"\n"
    add2file.write(mytext)
    add2file.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser() # ArgumentParser allows the use of command line args

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-l', '--flist', help="File list in lieu of directory")
    # group.add_argument('--foobar', nargs=2, metavar=('FOO', 'BAR'))
    group.add_argument(
        '-d', '--idir', help='Input directory to recursively reformat.')
    parser.add_argument("-v", "--verify", help="Decompression check performed",
                        action="store_true")
    parser.add_argument("-t", "--threads", type=int, help="Number of threads")

    args = parser.parse_args()
    print('Parsed arguments:')
    print(args)
    print_date = "\n\n**********  {} **********\n".format(datetime.datetime.now())
    print(print_date)
    logging.info(print_date)
    logging.debug('About to call main()') # track progress
    logging.info('args: {}'.format(args))
    main(**vars(args))
