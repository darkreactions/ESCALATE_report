"""A clean workaround for setting globals from the UI

Usage:
    * call a setter function from runme
        * you can only call a setter function once
    * use the corresponding getter function to get the variable from anywhere!
    * don't touch the private vars!
"""
import logging
import sys

modlog = logging.getLogger('utils.globals')


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