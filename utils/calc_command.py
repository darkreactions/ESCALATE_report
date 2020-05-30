""" Examples
>>>> Simple arithmetic operation
    "_raw_molfraction_acid" : {
        "command" : "a / (a + b)", # each variable should be defined in variable names
        "variable_names": {
            "a" : "_rxn_molarity_acid", # column names should come from, "./<my_local_folder>/logging/POTENTIAL_CALC_COLUMNS.txt"
            "b" : "_raw_solvent_0_molarity"
        },
        "description" : "mol weighted fraction of acid out of acid+solvent", 
        "fill_value" : 0 
    }
>>>> Using specified function
    "_calc_acid_solvent_average_hansen_deltap" : {
        "command" : "mean(a)",
        "variable_names": {
            "a" : ["_feat_acid_0_hansentriple_deltap_amount",
                   "_feat_solvent_0_hansentriple_deltap_amount"]
        },
        "functions" : {"mean": lambda x: x.mean()}, # .mean() is a pandas function which takes the average of the specified columns
        "description" : "weighted hansen deltap parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltap = acid(deltap)+ solvent(deltap)]", 
        "fill_value" : 0 
    }
"""
import pandas as pd
# Additional functions used by the calc_command_dict can be imported here

CALC_COMMAND_DICT = {
    "_raw_molfraction_acid" : {
        "command" : "a / (a + b)", 
        "variable_names": {
            "a" : "_rxn_molarity_acid", 
            "b" : "_raw_solvent_0_molarity"
        },
        "description" : "mol weighted fraction of acid out of acid+solvent", 
        "fill_value" : 0 
    },
    "_raw_molfraction_solvent" : {
        "command" : "b / (a + b)",
        "variable_names": {
            "a" : "_rxn_molarity_acid",
            "b" : "_raw_solvent_0_molarity"
        },
        "description" : "mol weighted fraction of solvent out of acid+solvent", 
        "fill_value" : 0 
    },
    "_calc_weightedhansen_deltad" : {
        "command" : "(a*b)+(c*d)",
        "variable_names": {
            "a" : "_feat_acid_0_hansentriple_deltad_amount",
            "b" : "_raw_molfraction_acid",
            "c" : "_feat_solvent_0_hansentriple_deltad_amount",
            "d" : "_raw_molfraction_solvent"
        },
        "description" : "weighted hansen deltad parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltad = acid(deltad)+ solvent(deltad)]", 
        "fill_value" : 0 
    },
    "_calc_weightedhansen_deltah" : {
        "command" : "(a*b)+(c*d)",
        "variable_names": {
            "a" : "_feat_acid_0_hansentriple_deltah_amount",
            "b" : "_raw_molfraction_acid",
            "c" : "_feat_solvent_0_hansentriple_deltah_amount",
            "d" : "_raw_molfraction_solvent"
        },
        "description" : "weighted hansen deltah parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltah = acid(deltah)+ solvent(deltah)]", 
        "fill_value" : 0 
    },
    "_calc_weightedhansen_deltap" : {
        "command" : "(a*b)+(c*d)",
        "variable_names": {
            "a" : "_feat_acid_0_hansentriple_deltap_amount",
            "b" : "_raw_molfraction_acid",
            "c" : "_feat_solvent_0_hansentriple_deltap_amount",
            "d" : "_raw_molfraction_solvent"
        },
        "description" : "weighted hansen deltap parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltap = acid(deltap)+ solvent(deltap)]", 
        "fill_value" : 0 
    },
    "_calc_acid_solvent_average_hansen_deltad" : {
        "command" : "mean(a)",
        "variable_names": {
            "a" : ["_feat_acid_0_hansentriple_deltad_amount",
                   "_feat_solvent_0_hansentriple_deltad_amount"]
        },
        "functions" : {"mean": lambda x: x.mean()}, # .mean() is a pandas function which takes the average of the specified columns
        "description" : "weighted hansen deltap parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltap = acid(deltap)+ solvent(deltap)]", 
        "fill_value" : 0 
    },
    "_calc_acid_solvent_average_hansen_deltah" : {
        "command" : "mean(a)",
        "variable_names": {
            "a" : ["_feat_acid_0_hansentriple_deltah_amount",
                   "_feat_solvent_0_hansentriple_deltah_amount"]
        },
        "functions" : {"mean": lambda x: x.mean()}, # .mean() is a pandas function which takes the average of the specified columns
        "description" : "weighted hansen deltap parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltap = acid(deltap)+ solvent(deltap)]", 
        "fill_value" : 0 
    },
    "_calc_acid_solvent_average_hansen_deltap" : {
        "command" : "mean(a)",
        "variable_names": {
            "a" : ["_feat_acid_0_hansentriple_deltap_amount",
                   "_feat_solvent_0_hansentriple_deltap_amount"]
        },
        "functions" : {"mean": lambda x: x.mean()}, # .mean() is a pandas function which takes the average of the specified columns
        "description" : "weighted hansen deltap parameter calculated from mol weighted acid and solvent (e.g. [experiment's solvent deltap = acid(deltap)+ solvent(deltap)]", 
        "fill_value" : 0 
    },
    "_feat_halide_electronegativity" : {
        "command" : "mean(a)",
        "variable_names": {
            "a" : "inorganic_._atomic_electronegativity_(?!pb)"
        },
        "functions" : {"mean": lambda x: x.mean()}, # takes the average
        "description" : "averages the halides!)", 
        "fill_value" : 0,
        "use_regex": True # Interpret variable_names as regex filter
    }
}