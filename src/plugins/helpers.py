# -*- coding: utf-8 -*-
''' Import statements '''
import sys, bz2, gzip, zipfile,  os
import rarfile
from decimal import Decimal, ROUND_HALF_UP
from os.path import splitext, basename, exists
from fuzzywuzzy import process
'''
    Used for finding environment variables through configuration
    if a default is not given, the site will raise an exception
'''
def get_env_variable(var_name, default=-1):
    try:
        return os.environ[var_name]
    except KeyError:
        if default != -1:
            return default
        error_msg = "Set the %s os.environment variable" % var_name
        raise Exception(error_msg)

def d(x):
  return Decimal(x).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)

def smart_try(Opener, file_obj, file_path_no_ext, tries):
    try:
        file_obj = Opener.open(file_obj, file_path_no_ext + tries.pop())
    except:
        if tries:
            return smart_try(Opener, file_obj, file_path_no_ext, tries)
        elif file_obj:
            filelist = file_obj.filelist
            for f in filelist:
                if file_path_no_ext.lower() in f.filename.lower():
                    return file_obj.open(f.filename)
            # as a last resort, try fuzzy matching
            choices = [f.filename for f in filelist]
            fname, pct = process.extractOne(file_path_no_ext, choices)
            return file_obj.open(fname)
    return file_obj

def raw_file_handle(full_path):
    print "FULL=", full_path
    file_name = basename(full_path)
    file_path_no_ext, file_ext = splitext(file_name)

    extensions = {
        '.bz2': bz2.BZ2File,
        '.gz': gzip.open,
        '.zip': zipfile.ZipFile,
        '.rar': rarfile.RarFile
    }
    
    try:
        file = extensions[file_ext](full_path)
    except KeyError:
        file = open(full_path)
    except IOError:
        return None
    
    return file

def get_file(full_path):
    print "FULL=", full_path
    file = raw_file_handle(full_path)

    kinds = ['', '.txt', '.csv', '.tsv']
    file_name = basename(full_path)
    file_path_no_ext, file_ext = splitext(file_name)

    if file_ext == '.zip':
        file_opened = smart_try(zipfile.ZipFile, file, file_path_no_ext, kinds)
        if file == file_opened:
            raise Exception("Could not get single file handle")
        file = file_opened
    elif file_ext == '.rar':
        file = smart_try(rarfile.RarFile, file, file_path_no_ext, kinds)
    # print "Reading from file", file_name
    return file

def format_runtime(x):
    # convert to hours, minutes, seconds
    m, s = divmod(x, 60)
    h, m = divmod(m, 60)
    if h:
        return "{0} hours and {1} minutes".format(int(h), int(m))
    if m:
        return "{0} minutes and {1} seconds".format(int(m), int(s))
    if s < 1:
        return "< 1 second"
    return "{0} seconds".format(int(s))

