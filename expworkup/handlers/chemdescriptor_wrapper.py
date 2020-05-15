import logging
import pandas as pd 

from chemdescriptor.generator.chemaxon import ChemAxonDescriptorGenerator as cag
from chemdescriptor.generator.rdkit import RDKitDescriptorGenerator as rdg

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def cxcalc_handler(type_feat_dict, cxcalc_command_dict, smiles_list, target_name, one_type, standardize_bool, log_folder):
    """
    """
    try:
    #TODO: update logs and fix outputfile meaningfully
        calc_features = cag(smiles_list,
                            whitelist={},
                            command_dict=cxcalc_command_dict,
                            logfile=f'{log_folder}/CXCALC_LOG.txt',
                            standardize=standardize_bool)
        type_features_df = \
            calc_features.generate(f'./{target_name}/offline/CXCALC_{one_type}_FEATURES.csv',
                                   dataframe=True,
                                   rename_columns=True)

        # Alert the user if the autorename didn't work.  
        # This is hard to automate, ChemAxon doesn't document what it returns
        feature_rename_list = []
        for feature in cxcalc_command_dict['descriptors'].keys():
            feature_rename_list.extend(cxcalc_command_dict['descriptors'][feature]['column_names'])

        # -1 to remove the smiles identity column from the count
        # if these are the same length, the chemaxon rename will go through
        # if not, the chemaxon default columns will be returned
        if (type_features_df.shape[1]-1) != len(feature_rename_list):
            modlog.warn(f'Rename of ChemAxon {one_type} descriptors was unsuccessful. Please verify user specified column names in type_command.csv!')
            warnlog.warn(f'Rename of ChemAxon {one_type} descriptors was unsuccessful. Please verify user specified names in type_command.csv!')

    except UnboundLocalError:
        modlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
        warnlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
        import sys
        sys.exit()

    type_feat_dict[one_type] = pd.concat([type_feat_dict[one_type], type_features_df], axis=1)
    return type_feat_dict

def rdkit_handler(type_feat_dict, rdkit_command_dict, smiles_list, target_name, one_type, log_folder):
    """
    """
    try:
        rdkit_features = rdg(smiles_list,
                             whitelist=rdkit_command_dict,
                             logfile=f'{log_folder}/RDKIT_LOG.txt')
        type_features_df = rdkit_features.generate(f'./{target_name}/offline/RDKIT_{one_type}_FEATURES.csv',
                                                   dataframe=True)
    except UnboundLocalError:
        modlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
        warnlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
        import sys
        sys.exit()

    feature_rename_list = []
    for feature in rdkit_command_dict['descriptors'].keys():
        feature_rename_list.extend(rdkit_command_dict['descriptors'][feature]['column_names'])

    if (type_features_df.shape[1]-1) > len(feature_rename_list):
        modlog.warn(f'RDKit features for {one_type} were incorrectly specified. Outputting ALL available options for the smiles. Please verify type_command.csv to reduce output')
        warnlog.warn(f'RDKit features for {one_type} were incorrectly specified. Outputting ALL available options for the smiles. Please verify type_command.csv to reduce output')

    type_feat_dict[one_type] = pd.concat([type_feat_dict[one_type], type_features_df], axis=1)
    # Drop duplicate columns on join
    type_feat_dict[one_type] = \
        type_feat_dict[one_type].loc[:,~type_feat_dict[one_type].columns.duplicated()]
    return type_feat_dict
    