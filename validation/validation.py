import pandas as pd
import numpy as np
import cerberus

from validation import schemas

V = cerberus.Validator()

def validate_crystal_scoring(crys_df):
    crys_dict = crys_df.to_dict(orient='list')
    is_valid = V.validate(crys_dict, schemas.CRYSTAL_FILE)
    print(V.errors)
    return is_valid

def validate_robot_input(pipette_volumes, reaction_parameters, reagent_info):
    pipette_volumes = pipette_volumes.to_dict(orient='list')
    is_valid = V.validate(pipette_volumes, schemas.ROBO_FILE_PIPETTE_VOLUMES)
    print(V.errors)
    return is_valid
