#Copyright (c) 2020 Ian Pendleton - MIT License
import os
import logging
import numpy as np
import pandas as pd
from tqdm import tqdm

from expworkup.handlers.chemical_types import get_chemical_types
from chemdescriptor.generator.chemaxon import ChemAxonDescriptorGenerator as cag
from chemdescriptor.generator.rdkit import RDKitDescriptorGenerator as rdg
from utils.file_handling import write_debug_file

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def get_command_dict(command_type_df, one_type, application):
    """Converts expworkup.type_command.csv to dict for chemdescriptor

    Parameters
    ----------
    command_type_df : pd.DataFrame generated from type_command.csv

    one_type : defines which chemical type to target
        should match an entry in command_types_df 'types' column 
    
    application : defines the application being targeted by caller
        will only return rows where actor_systemtool_name matches 
        specified application

    Returns
    -------
    default_command_dict : structure shown below
        default_command_dict = {
        "descriptors": {
            "acceptorcount": {
                "command": [
                    "acceptorcount"
                ],
                "column_names": [
                    "_feat_acceptorcount"
                ]
            },...
        ""ph_descriptors": {
            "molsurfaceareaASAp": {
                "command": [
                    "molecularsurfacearea",
                    "-t",
                    "ASA+"
                ],
                "column_names": [
                    "_feat_molsurfaceareaASAp"
                ]
            },...

    Notes
    -----
     * https://github.com/darkreactions/chemdescriptor
     * 'descriptors' must be specified fully (including flags where needed)
     * 'ph_descriptors' are those which have -H option, can use to simplify return

    """
    commands_df = command_type_df[(command_type_df['input'] == one_type) &  \
                                  (command_type_df['actor_systemtool_name'] == application)]

    my_descriptor_dict = {}
    for command in commands_df.itertuples():
            column_name = f'_feat_{command.short_name}'
            my_descriptor_dict[command.short_name] = {}
            # stupid human space removal
            templist = command.calc_definition.split(' ')
            str_list = list(filter(None, templist))
            my_descriptor_dict[command.short_name]["command"] = str_list

            my_descriptor_dict[command.short_name]["column_names"] = [column_name]
    command_dict = {}
    command_dict['descriptors'] = my_descriptor_dict
    command_dict['ph_descriptors'] = {} # possibly useful, see chemdescriptor for more details
    if len(command_dict['descriptors'].keys()) == 0:
        return None
    else:
        return(command_dict)

def get_features(unique_types, experiment_inchi_df, target_name, log_folder):
    """ gather specified features to describe all expUID+inchi+type combinations
    All features for experimentUID + InchiKey + types combinations across
    labs are gathered and returned as a dict of pd.Dataframes 

    Parameters
    ----------
    unique_types : set of chemical_types for the full dataset

    experiment_inchi_df : pd.Dataframe with [expUID, inchikeys, types]
        which contains all unique combinations of expUID+inchikeys 
        along with associated type list for each

    target_name : name of the working data directory
        used for report logs 

    log_folder : folder location to stores logs 
    
    Returns
    -------
    type_feat_dict : dict of pd.DataFrames containing features
        keys:one_type label, values:dfs of features from chemdescriptor

    """
    #TODO: fix logging issues

    # TODO: insert validation of the type_command df
    # type_command df should be treated as code!
    type_command_df = pd.read_csv('./type_command.csv')
    type_feat_dict = {}

    modlog.info(f'Generating physicochemical features to {target_name} dataset')
    modlog.info(f'Log files for this process are in CXCALC_LOG.txt')
    print(f'(5/6) Gathering physicochemical features for {len(unique_types)} chemical type(s) in {target_name}...')
    for one_type in tqdm(unique_types):
        #only grabs EXACT string matches for the type
        correct_type =\
            experiment_inchi_df[experiment_inchi_df['types']\
            .str.contains(pat=f'(?:^|\W){one_type}(?:$|\W)',\
                          regex=True)]
        #gather information we will want in the final dataframe for features
        inchi_smiles = correct_type[['inchikeys', 'smiles', 'types']]\
                                   .drop_duplicates(keep="first", 
                                                    ignore_index=True)
        type_feat_dict[one_type] = inchi_smiles
        smiles_list = inchi_smiles['smiles'].values.tolist()
        cxcalc_command_dict = get_command_dict(type_command_df,
                                             one_type,
                                             'cxcalc')
        rdkit_command_dict = get_command_dict(type_command_df,
                                              one_type,
                                              'RDKit')
        if cxcalc_command_dict is not None:
            try:
            #TODO: update logs and fix outputfile meaningfully
                calc_features = cag(smiles_list,
                                    whitelist={},
                                    command_dict=cxcalc_command_dict,
                                    logfile=f'{log_folder}/CXCALC_LOG.txt',
                                    standardize=True)
                type_features_df = calc_features.generate(f'./{target_name}/offline/CXCALC_{one_type}_FEATURES.csv',
                                                          dataframe=True,
                                                          lec=False)
            except UnboundLocalError:
                modlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
                warnlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
                import sys
                sys.exit()
            type_feat_dict[one_type] = pd.concat([type_feat_dict[one_type], type_features_df], axis=1)
        if rdkit_command_dict is not None:
            try:
#                rdkit_whitelist= rdkit_command_dict['descriptors'].keys()
                rdkit_features = rdg(smiles_list,
                                     whitelist=rdkit_command_dict,
#                                     command_dict=rdkit_command_dict,
                                     logfile=f'{log_folder}/RDKIT_LOG.txt')
                
                type_features_df = rdkit_features.generate(f'./{target_name}/offline/RDKIT_{one_type}_FEATURES.csv',
                                                           dataframe=True)
            except UnboundLocalError:
                modlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
                warnlog.error(f'Critical Error: cxcalc functions incorrectly specified. Please validate type_command.csv!')
                import sys
                sys.exit()
            type_feat_dict[one_type] = pd.concat([type_feat_dict[one_type], type_features_df], axis=1)
            # Drop duplicate columns on join
            type_feat_dict[one_type] = \
                type_feat_dict[one_type].loc[:,~type_feat_dict[one_type].columns.duplicated()]
        try:
#            type_feat_dict[one_type] = pd.concat([type_feat_dict[one_type], inchi_smiles], axis=1)
            type_feat_dict[one_type].rename(columns={"Compound": "smiles_standardized"}, inplace=True)
        except KeyError:
            modlog.warn(f'No features defined for {one_type}')
            pass

    modlog.info(f"Completed: 'Generating physicochemical features to {target_name} dataset'")
    return(type_feat_dict)

def unpack_features(type_feat_dict):
    """ Digest dictionary of features and convert to enumerated dataframe

    Parameters
    ----------
    type_feat_dict : dict of pd.DataFrames containing features
        keys:one_type label, values:dfs of features from chemdescriptor
    
    Returns
    ----------
    inchi_key_indexed_features_df : pd.DataFrame with all features
        features are indexed on the inchikeys and contain columns
        associated with all computed features. Empty cells are returned
        for intersections with no values.  

    Notes
    ----------
    * Inchikeys that are associated with multiple chemical_types will merge
    to a single row (merge down) see notes on pd.DataFrame.combine_first()
    for more information
    """
    inchi_key_indexed_features_df = pd.DataFrame()
    for one_type in type_feat_dict:
        df = type_feat_dict[one_type].set_index('inchikeys')

        # Merge the dataset into the existing index DOES NOT OVERWRITE
        inchi_key_indexed_features_df = \
            inchi_key_indexed_features_df.combine_first(df)
    inchi_key_indexed_features_df.drop_duplicates(subset=['smiles','smiles_standardized'],
                                                  inplace=True, keep='first')
    return(inchi_key_indexed_features_df)

def feat_pipeline(target_name, report_df, chem_df_dict, debug_bool, log_folder):
    """ Manage ingest of lab content and return feature dataframes

    Parameters
    ----------
    target_name : name of the target folder (and final curated csv output)

    report_df : pandas.DataFrame
        dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline

    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction

    debug_bool : CLI argument, True=Enable debugging
        if toggled on, code will export CSV files of each dataframe

    log_folder : folder location to stores logs

    Returns
    -------
    ()
    
    """
    report_copy = report_df.copy().set_index('name')

    # Build a dataframe of every unique inchikey
    inchi_df = report_copy.filter(regex='reagent_._chemicals_._inchikey')
    inchi_df = inchi_df.dropna(how='all', axis=1)
    inchi_df.fillna(value='null', inplace=True)
    unique_inchis_series = inchi_df.apply(lambda x: list(set(x)), axis = 1)
    unique_inchis_df = unique_inchis_series.to_frame(name='inchikeys')
    unique_inchis_df['lab'] = report_copy['_raw_lab']
    unique_inchis_df.reset_index(inplace=True)

    # creates a stacked dataframe with each runUID and inchikey combination as a unique row
    expanded_unique_inchis_df = unique_inchis_df.explode('inchikeys')
    expanded_unique_inchis_df['name'] = expanded_unique_inchis_df.index
    expanded_unique_inchis_df = expanded_unique_inchis_df[expanded_unique_inchis_df['inchikeys'] != 'null']

    # add chemicals_types ('types') and the associated smiles based on inchikey
    # requires that the correct lab is provided (the lab is identified in the runUID)
    expanded_unique_inchis_df[['types','smiles']] = \
        expanded_unique_inchis_df.apply(lambda x: get_chemical_types(x['inchikeys'], 
                                                                     x['lab'],
                                                                     chem_df_dict),
                                                                            axis=1)
    # a bit of ugly to remove the list structure in types column
    expanded_unique_inchis_df['types'] = \
        [','.join(map(str, l)) for l in expanded_unique_inchis_df['types']]
    # Get only the unique instances of each chemical types in the dataset
    ugh_list = list(entry for entry in expanded_unique_inchis_df['types'].values.tolist())
    #Unpack potential list of lists from 'types' column and return uniques
    unique_types = list(set([x for l in ugh_list for x in l.split(',')]))
    unique_types.sort()
    type_feat_dict = get_features(unique_types,
                                  expanded_unique_inchis_df,
                                  target_name,
                                  log_folder)
    
    ## Output dataframe with association between unique inchikeys and features
    inchi_key_indexed_features_df = unpack_features(type_feat_dict)
    inchi_key_indexed_features_df.groupby("inchikeys").ffill()\
                                 .groupby("inchikeys").last()\
                                 .reset_index(inplace=True)

    ## Output dataframe with runID x inchi column(s)
    # rename list of inchis to a column / inchi 
    runUID_indexed_inchikey_df = \
        unique_inchis_df['inchikeys'].apply(pd.Series)
    #unpack set(type=list) of inchis into unique columns
    runUID_indexed_inchikey_df['name'] = unique_inchis_df['name']
    runUID_indexed_inchikey_df.set_index('name',
                                         inplace=True)
    runUID_indexed_inchikey_df.replace(to_replace='null', value=np.nan, inplace=True)
    # remove null columns and align, order here doesn't matter
    runUID_indexed_inchikey_df = \
        runUID_indexed_inchikey_df.apply(lambda x: x.dropna().reset_index(drop=True), axis=1)
    runUID_indexed_inchikey_df.dropna(axis=1, inplace=True, how='all')
    runUID_indexed_inchikey_df = \
        runUID_indexed_inchikey_df.rename(columns = \
                                          lambda x : 'inchikey_' + str(x))

    ###### Dataframe Export Begins Here ####### 
    if debug_bool:
        # Export dataframes physicochemical features and load tables for ETL to ESCALATEV3
        inchi_key_indexed_features_df_file = 'REPORT_INCHI_FEATURES_TABLE.csv'
        write_debug_file(inchi_key_indexed_features_df,
                         inchi_key_indexed_features_df_file)
        # Export dataframe for linking inchikeys to runUIDS for V3 ETL
        runUID_inchi_file = 'REPORT_UID_LOADTABLE.csv'
        write_debug_file(runUID_indexed_inchikey_df,
                         runUID_inchi_file)
    return runUID_indexed_inchikey_df, inchi_key_indexed_features_df

