"""Schemas

Here is a quick demo of using cerberus style schemas for the crystal file
and
TODO: Not clear if this is accurate anymore (April 6, 2020), clarify, update
"""
CRYSTAL_FILE = {

    # we have only specified any actual rules about these two
    'Crystal Score': {
        'type': 'list',
        'schema': {
            'type': 'string',
            'allowed': ['1', '2', '3', '4']
        },
    },

    'Bulk Actual Temp (C)': {
        'type': 'list',
        'schema': {
            'type': 'string',
            'empty': False},
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
}


ROBO_FILE_REAGENT_INFO = {
    'Reagents': {'type': 'list'},
    'Reagent identity': {'type': 'list'},
    'Liquid Class': {'type': 'list'},
    'Reagent Temperature': {'type': 'list'}
}

ROBO_FILE_PIPETTE_VOLUMES = {
    'Vial Site':     {'type': 'list', 'schema': {'type': 'string'}},
    'Reagent1 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Reagent2 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Reagent3 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Reagent4 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Reagent5 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Reagent6 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Reagent7 (ul)': {'type': 'list', 'schema': {'type': 'integer'}},
    'Labware ID:':   {'type': 'list', 'schema': {'type': 'string'}},
}

ESCALATE_RUN = {'robo': {'reagents': ROBO_FILE_REAGENT_INFO}}

