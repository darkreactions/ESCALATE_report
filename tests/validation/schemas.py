"""Schemas

Some functional demos of cerbus style validation.. can be added by\
devs for labs as needed

Try to add fixes to the data import rather than changing validation. 
If a general fix can be programmed, do that. If not, change the source.

Some useful links for validation:
General documentation frontpage: https://docs.python-cerberus.org/en/stable/index.html#
Types, examples, info https://docs.python-cerberus.org/en/stable/validation-rules.html#type

"""
import numpy as np

CRYSTAL_FILE = {
    # we have only specified any actual rules about these two
    'Crystal Score': {
        'type': 'list',
        'schema': {
            'type': ['integer', 'string'],
            'allowed': [1, 2, 3, 4, 'null']
        },
    },

    'Bulk Actual Temp (C)': {
        'type': 'list',
        'schema': {
            'type': ['string', 'integer', 'number']
            }
    },
    'Robot Number': {'type': 'list'},
    'Vial Site (1)': {'type': 'list'},
    'Vial Site (2)': {'type': 'list'},
    'Concatenated Vial site': {'type': 'list'},
    'identifier': {'type': 'list'},
    '_out_predicted': {'type': 'list'},
    'modelname': {'type': 'list'},
    'participantname': {'type': 'list'},
    'notes': {'type': 'list'},
    'class_probability': {'type': 'list'},
    'Experiment Name': {'type': 'list'}
}

# This one is a doozy, but important.. 
EXPERIMENTAL_DATA = {
}
#    'run': {
#        'type': 'list', 
#        'schema': {'type': ['string', 'number']}},
#    'notes': {
#        'type': 'list', 
#        'schema': {'type': 'string'}}

REACTION_PARAMETERS_SCHEMA = {
    'Reaction Parameters': {'type': 'list', 'schema':{'type': 'string'}},
    'Parameter Values': {'type': 'list', 'schema':{'type': 'number'}}
}

REAGENT_INFORMATION_SCHEMA = {
    'Reagents':     {'type': 'list', 'schema': {'type': 'string'}},
    'Reagent identity': {'type': 'list', 'schema': {'type': ['number', 'string']}},
    'Liquid Class': {'type': 'list', 'schema': {'type': 'string'}},
    'Reagent Temperature': {
        'type': 'list',
        'schema': {
            'oneof': [
            {'type': 'number'},
            {'type': 'string', 'allowed': ['null', 'NULL', 'Null']}
            ]
         }
    }
}

ROBO_FILE_PIPETTE_VOLUMES = {
    'Vial Site':     {'type': 'list', 'schema': {'type': 'string'}},
    'Reagent1 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent2 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent3 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent4 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent5 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent6 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent7 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent8 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Reagent9 (ul)': {'type': 'list', 'schema': {'type': 'number'}},
    'Labware ID:':   {'type': 'list', 'schema': {'type': 'string'}}
}

