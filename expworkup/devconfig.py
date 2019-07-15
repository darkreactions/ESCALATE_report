# report devconfig.py
import os
import sys

cwd = os.getcwd()

settings = {
    "notes": {
        "ExperimentalSummary": "Stock solution preparation different than workflow 1 starting in 2018. Grind up PbI2 into a fine powder and dissolve in GBL stirring at 60C for 30 minute. Dissolve amine into GBL and heat until dissolved, combine reagents to form the primary stock solution at the desired ratio. Add amine solution to PbI2 GBL solution and stir for 45 minute then filter stock solution using PTFE 0.45 \u03bcm filter. ",
        "note1": "Difference in workflow is not captured by this reaction JSON file. Specifically the operational details of preparing the stock solution.",
        "note2": "null"
    },
    debug_dictionary = {}
    mit_dictionary = {
        'experimental_observation_interface' {
            "Transparent?": "_rxn_transparent"
        },
        'target_data_folder':  '<enter UID> '
    }
}
