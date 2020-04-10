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

modlog = logging.getLogger('report.globals')

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