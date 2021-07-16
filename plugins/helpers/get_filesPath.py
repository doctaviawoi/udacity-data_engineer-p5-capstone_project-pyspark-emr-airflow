import os
import glob
from pathlib import Path

files = {}

def get_filesPath(files_path, recursive=False):
    """
    This function scans for files in a directory and returns dictionary of files that is not .pyc as key and full path as its value,
    eg. {<filename1>: </path/to/file1>, <filename2>: <path/to/file2>}

    Args:
        files_path (str): File path directory to scan.
        recursive (bool): Option to scan the directory recursively, defaulted to False.

    Returns:
        files (dictionary): Dictionary of filenames in the directory as key and file full directory path as value.

    """
    if recursive:
        for i in os.scandir(files_path):
            if i.is_file() and i.path[-3:] != 'pyc':
                fname = i.path.replace(os.sep, '/').split('/')[-1]
                files[fname] = i.path.replace(os.sep, '/')

            elif i.is_dir():
                get_filesPath(i.path.replace(os.sep, '/'))

    else:
        for i in glob.glob(os.path.join(files_path, '*')):
            if Path(i).is_file() and i[-3:] != 'pyc':
                fname = i.replace(os.sep, '/').split('/')[-1]
                files[fname] = i.replace(os.sep, '/')

    return files
