import pandas as pd
import numpy as np
import logging
import json
import cerberus

from tests.validation import schemas
from utils.globals import WARNCOUNT
from utils.globals import get_log_folder

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')
V = cerberus.Validator()


def validate_observation_interface(crys_df):
    global WARNCOUNT
    crys_dict = crys_df.to_dict(orient='list')
    V.allow_unknown = True #Allows for unspecified columns, flexible interface and likely to happen
    is_valid = V.validate(crys_dict, schemas.CRYSTAL_FILE)
    if not is_valid:
        modlog.info(V.errors)
        if WARNCOUNT == 0:
            warnlog.warn(f'Files failed to validate. Please check: {get_log_folder()}/REPORT_LOG.txt')
            WARNCOUNT += 1
    return is_valid

def validate_experimental_volumes(experiment_volumes):
    global WARNCOUNT
    experiment_volumes = experiment_volumes.to_dict(orient='list')
    is_valid = V.validate(experiment_volumes, schemas.ROBO_FILE_PIPETTE_VOLUMES)
    if not is_valid:
        modlog.info(V.errors)
        if WARNCOUNT == 0:
            warnlog.warn(f'Files failed to validate. Please check: {get_log_folder()}/REPORT_LOG.txt')
            WARNCOUNT += 1
    return is_valid

def validate_reaction_parameters(reaction_parameters):
    global WARNCOUNT
    reaction_parameters = reaction_parameters.to_dict(orient='list')
    is_valid = V.validate(reaction_parameters, schemas.REACTION_PARAMETERS_SCHEMA)
    if not is_valid:
        modlog.info(V.errors)
        if WARNCOUNT == 0:
            warnlog.warn(f'Files failed to validate. Please check: {get_log_folder()}/REPORT_LOG.txt')
            WARNCOUNT += 1
    return is_valid

def validate_reagent_info(reagent_info):
    global WARNCOUNT
    reagent_info = reagent_info.to_dict(orient='list')
    is_valid = V.validate(reagent_info, schemas.REAGENT_INFORMATION_SCHEMA)
    if not is_valid:
        modlog.info(V.errors)
        if WARNCOUNT == 0:
            warnlog.warn(f'Files failed to validate. Please check: {get_log_folder()}/REPORT_LOG.txt')
            WARNCOUNT += 1
    return is_valid

def validate_ingredient_data(ingredient_data):
    #TODO decide whether this validation should happen in the interface or 
    #    here.  Design the validation.  (IMO (ian) should be on interface..)
    # The validation is CURRENTLY EMPTY
    global WARNCOUNT
    V.allow_unknown = True #Allows for unspecified columns, flexible interface and likely to happen
    is_valid = V.validate(ingredient_data, schemas.EXPERIMENTAL_DATA) 
    if not is_valid:
        modlog.info(V.errors)
        if WARNCOUNT == 0:
            warnlog.warn(f'Files failed to validate. Please check: {get_log_folder()}/REPORT_LOG.txt')
            WARNCOUNT += 1
    return is_valid

def validate_is_json(target_json_file):
    """ Ensure that the JSON file can be parsed

    Parameters
    ----------
    target_json_file : json file to inspect

    Returns
    -------
    is_json : Bool, if parseable, return true
    """
    try:
        dummy_holder = json.load(open(target_json_file, 'r'))
        return True
    except Exception:
        return False