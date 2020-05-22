#Copyright (c) 2020 Ian Pendleton - MIT License
import os
import logging
import numpy as np
import pandas as pd
from tqdm import tqdm

from expworkup.handlers.chemical_types import get_chemical_types
from expworkup.handlers.feature_generator import OneTypeFeatures
from utils.file_handling import write_debug_file

from utils.globals import get_target_folder

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def get_features(unique_types, experiment_inchi_df):
    """ gather specified features to describe all expUID+inchi+type combinations
    TODO: update this docstring
    All features for experimentUID + InchiKey + types combinations across
    labs are gathered and returned as a dict of pd.Dataframes 

    Parameters
    ----------
    unique_types : set of chemical_types for the full dataset

    experiment_inchi_df : pd.Dataframe with [expUID, inchikeys, types]
        which contains all unique combinations of expUID+inchikeys 
        along with associated type list for each

    Returns
    -------
    type_feat_dict : dict of pd.DataFrames containing features
        keys:one_type label, values:dfs of features from chemdescriptor

    """
    modlog.info(f'Generating physicochemical features to {get_target_folder()} dataset')
    modlog.info(f'Log files for this process are in CXCALC_LOG.txt')
    print(f'(5/6) Gathering physicochemical features for {len(unique_types)} chemical type(s) in {get_target_folder()}...')
    type_feat_dict = {}
    for one_type in tqdm(unique_types):
        #only grabs EXACT string matches for the type
        correct_type = \
            experiment_inchi_df[experiment_inchi_df['types'].\
                                str.contains(pat=f'(?:^|\W){one_type}(?:$|\W)',
                                regex=True)]
        #gather information we will want in the final dataframe for features
        onetype_feature_identity_dict = correct_type[['inchikeys', 'smiles', 'types']].\
                                                     drop_duplicates(keep="first", 
                                                     ignore_index=True)
        type_feat_dict[one_type] = OneTypeFeatures(one_type, 
                                                   onetype_feature_identity_dict)

    modlog.info(f"Completed: 'Generating physicochemical features to {get_target_folder()} dataset'")
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
        # Dictionary of expworup.handlers.feature_generator.OneTypeFeatures 
        df = type_feat_dict[one_type].featured_df.copy()

        # Merge the dataset into the existing index DOES NOT OVERWRITE
        inchi_key_indexed_features_df = \
            df.combine_first(inchi_key_indexed_features_df)#.combine_first(df)

    inchi_key_indexed_features_df.reset_index(inplace=True)
    duplicate_targets = ['smiles', 'smiles_standardized', 'types', 'inchikeys']
    shared_cols = list(frozenset(inchi_key_indexed_features_df.columns).intersection(duplicate_targets))
    inchi_key_indexed_features_df.drop_duplicates(subset=shared_cols, inplace=True, keep='first')
    inchi_key_indexed_features_df.set_index('inchikeys', inplace=True) 
    return(inchi_key_indexed_features_df)

def feat_pipeline(report_df, chem_df_dict, debug_bool):
    """ Manage ingest of lab content and return feature dataframes

    Parameters
    ----------
    report_df : pandas.DataFrame
        dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline

    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction

    debug_bool : CLI argument, True=Enable debugging
        if toggled on, code will export CSV files of each dataframe

    Returns
    -------
    runUID_indexed_inchikey_df : pandas.DataFrame
        all unique inchikeys for a run indexed in the runUID
        runs with less than the maximum will have np.nan values (blanks)

    inchi_key_indexed_features_df : pandas.DataFrame 
        all features selected by user in type_command.csv indexed on InchiKey
        headers will conform to the type_command.csv unless mismatched
        (mismatch occurs when the requested chemaxon features generate multiples)

    Notes
    -----

    NOTE: An easy way to get a view of each of the large dataframes is to enable
    debugging!  Each render will be cast to a simlar named csv.  Search for name
    for associated code or vice-versa.
    
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
                                  expanded_unique_inchis_df)
    
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
    #NOTE: An easy way to get a view of each of the large dataframes is to enable
    #debugging!  Each render will be cast to a simlar named csv.  Search for name
    #for associated code or vice-versa.
    if debug_bool:
        # Export dataframes physicochemical features and load tables for ETL to ESCALATEV3
        inchi_key_indexed_features_df_file = 'REPORT_INCHI_FEATURES_TABLE.csv'
        write_debug_file(inchi_key_indexed_features_df,
                         inchi_key_indexed_features_df_file)
        # Export dataframe for linking inchikeys to runUIDS for V3 ETL
        runUID_inchi_file = 'REPORT_UID_LOADTABLE.csv'
        write_debug_file(runUID_indexed_inchikey_df,
                         runUID_inchi_file)

    return inchi_key_indexed_features_df

