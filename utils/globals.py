"""A clean workaround for setting globals from the UI

Usage:
    * call a setter function from runme
        * you can only call a setter function once
    * use the corresponding getter function to get the variable from anywhere!
    * don't touch the private vars!
"""
import logging
import sys
import pandas as pd

from expworkup.ingredients.compound_ingredient import CompoundIngredient

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

WARNCOUNT = 0

TARGET_NAMING_SCHEME = None
TARGET_NAMING_SCHEME_SET = False
LOG_DIRECTORY = None
LOG_DIRECTORY_SET = False
OFFLINE_FOLDER = None
OFFLINE_FOLDER_SET = False
DEBUG_SIMPLE = None
DEBUG_SIMPLE_SET = False

_DEBUG_HEADER = None
_DEBUG_SET = False

def lab_safeget(dct, lab_key, key_1):
    '''
    used for retrieving either the default values or lab specific if specified
    from devconfig/lab_vars dictionary
    
    :param dct: lab_vars dictionary (includes default as well, from devconfig)
    :keys: key entries that are associted with the query (i.e. chemsheetid)

    :return either specified dictionary, or default lab dictionary if key error
    '''
    try:
        dct = dct[lab_key][key_1]
    except KeyError:
        dct = dct['default'][key_1]
    return dct

def compound_ingredient_chemical_return(ingredient, chemical_count, compoundingredient_func):
    """ Parse compoundingredients class and return specified function if appropriate (chemical)

    wrapper for reading chemical information from the compoundingredient
    class. This is required specifically for reading out the object_df 
    information as many of the entries are 'None".  

    Parameters
    ----------
    ingredient : a single instance of CompoundIngredient
    chemical_count : maximum number of chemicals in target datasets
        Specifically, this is the maximum number of chemicals across all
        experiments, in all datasets specified at the CLI
    compoundingredient_func : target function in CompoundIngredient
        function will return 1 instance per chemical, be sure to target
        only 'chemical lists'. See CompoundIngredient for more details

    Returns
    ----------
    CompoundIngredients.compoundingredient_func : type=list, len=chemical_count
        an ordered list of chemical descriptions (0-chemical_count) for a 
        CompoundIngredient.compoundingredient_func , these will primarly be
        the concentration and InChI keys of the chemicals in the order specified
        in report_df
    """
    if isinstance(ingredient, CompoundIngredient):
        ordered_conc_list = getattr(ingredient, compoundingredient_func)
        diff = chemical_count-len(ordered_conc_list)
        ordered_conc_list.extend([0]*diff)
        return(pd.Series(ordered_conc_list))
    else:
        return(pd.Series([0]*chemical_count))

def set_debug_header(header_string):
    global _DEBUG_HEADER, _DEBUG_SET
    if _DEBUG_SET:
        modlog.error('dev tried to run set_debug_header more than once')
        sys.exit(1)
    _DEBUG_HEADER = header_string
    _DEBUG_SET = True

def get_debug_header():
    if _DEBUG_HEADER is None:
        modlog.error('get_debug_header called before set_debug_header')
        sys.exit(1)
    return _DEBUG_HEADER

def set_target_folder_name(target_name):
    global TARGET_NAMING_SCHEME, TARGET_NAMING_SCHEME_SET
    if TARGET_NAMING_SCHEME_SET:
        modlog.error('dev tried to set target folder more than once')
        sys.exit(1)
    TARGET_NAMING_SCHEME = target_name
    TARGET_NAMING_SCHEME_SET = True

def get_target_folder():
    if TARGET_NAMING_SCHEME is None:
        modlog.error('get_target_folder called before set_target_folder_name')
        sys.exit(1)
    return TARGET_NAMING_SCHEME

def set_log_folder(log_target):
    global LOG_DIRECTORY, LOG_DIRECTORY_SET
    if LOG_DIRECTORY_SET:
        modlog.error('dev tried to set log folder more than once')
        sys.exit(1)
    LOG_DIRECTORY = log_target
    LOG_DIRECTORY_SET = True

def get_log_folder():
    if LOG_DIRECTORY is None:
        modlog.error('get_log_folder called before set_log_folder')
        sys.exit(1)
    return LOG_DIRECTORY

def set_offline_folder(offline_target_folder):
    global OFFLINE_FOLDER, OFFLINE_FOLDER_SET
    if OFFLINE_FOLDER_SET:
        modlog.error('dev tried to set offline folder more than once')
        sys.exit(1)
    OFFLINE_FOLDER = offline_target_folder
    OFFLINE_FOLDER_SET = True

def get_offline_folder():
    if OFFLINE_FOLDER is None:
        modlog.error('get_offline_folder called before set_offline_folder')
        sys.exit(1)
    return OFFLINE_FOLDER

def set_debug_simple(debug_simple_arg):
    global DEBUG_SIMPLE, DEBUG_SIMPLE_SET
    if DEBUG_SIMPLE_SET:
        modlog.error('dev tried to set simple debug more than once!')
        sys.exit(1)
    DEBUG_SIMPLE = debug_simple_arg
    DEBUG_SIMPLE_SET = True

def get_debug_simple():
    if DEBUG_SIMPLE is None:
        modlog.error('get_debug_simple called before set_debug_simple')
        sys.exit(1)
    return DEBUG_SIMPLE

