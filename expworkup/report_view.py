import logging
import pandas as pd

from utils.file_handling import write_debug_file
from expworkup.handlers.cleaner import cleaner
from expworkup.handlers.chemical_types import get_unique_chemicals_types_byinstance, runuid_feat_merge

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def construct_2d_view(report_df, 
                      amounts_df,
                      inchi_key_indexed_features_df,
                      ratios_df,
                      calcs_df,
                      debug_bool,
                      raw_bool):
    """ Combines the generated dataframes into a single 2d csv for export

    Parameters
    ----------
    report_df : pandas.DataFrame
        2d dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline
    
    amounts_df :  pandas.DataFrame of concatenated calculations
        does not include the report_df. Includes, chemical types, values
        indexed on runUID
    
    inchi_key_indexed_features_df : pandas.DataFrame 
        all features selected by user in type_command.csv indexed on InchiKey
        headers will conform to the type_command.csv unless mismatched
        (mismatch occurs when the requested chemaxon features generate multiples)
    
    ratios_df : pandas.DataFrame 
        calculated ratios of molarity from the _calc_ pipeline
        indexed on runUID ('name')
        columns are the ratio headers e.g. '_calc_ratio_acid_molarity_inorganic_molarity'
    
    calcs_df : pd.DataFrame
        completed _calcs_ specified by the ./utils/calc_command.py file
        indexed on runUID ('name')
        columns are the values return from _calcs_

    debug_bool : CLI argument, True=Enable debugging
        if toggled on, code will export CSV files of each dataframe
    
    raw_bool_cli : Bool, from CLI, include all columns?
        True will enable even improperly labeled columns to be exported
        proper labels can be defined in 'dataset_rename.json'

    Returns
    -------
    final_df : pandas.DataFrame with default view of data
        default view also removes all nan columns and all '0' columns
    TODO: add additional views (likely better in v3 though...)
        
    Notes
    -----
    NOTE: An easy way to get a view of each of the large dataframes is to add 
    '--debug 1' to the CLI!  Each render will be cast to a simlar named csv. 
    Search for name for associated code or vice-versa.
    """
    modlog.info("Generating 2d dataframe")
    print(f'Exporting 2d Dataframe...')

    # Some final exports for ETL in V3
    res = amounts_df.pivot_table(index=['name','inchikey'], 
                                  values=['molarity'],
                                  columns=['main_type'],
                                  aggfunc='sum')

    res.columns = res.columns.droplevel(0) # remove molarity top level
    sumbytype_molarity_df = res.groupby(level=0).sum()  ##**
    if debug_bool:
        sumbytype_molarity_df_file = 'REPORT_MOLARITY_BYTYPE_CALCS.csv'
        write_debug_file(sumbytype_molarity_df, sumbytype_molarity_df_file)
    sumbytype_molarity_df = sumbytype_molarity_df.add_prefix('_rxn_molarity_')

    sumbytype_byinstance_molarity_df = get_unique_chemicals_types_byinstance(res) ##**
    sumbytype_byinstance_molarity_df = \
        sumbytype_byinstance_molarity_df.add_prefix('_raw_')
    if debug_bool:
        sumbytype_byinstance_molarity_df_file = \
            'REPORT_MOLARITY_BYTYPE_BYINSTANCE_CALCS.csv'
        write_debug_file(sumbytype_byinstance_molarity_df, 
                         sumbytype_byinstance_molarity_df_file)

    feats_df = runuid_feat_merge(sumbytype_byinstance_molarity_df,
                                 inchi_key_indexed_features_df)

    # Generate a _raw_mmol_inchikey value for each inchikey in dataset
    mmol_inchi_df = amounts_df.pivot_table(index=['name'],
                                           values=['mmol'],
                                           columns=['inchikey'],
                                           aggfunc='sum')
    mmol_inchi_df.columns = mmol_inchi_df.columns.droplevel(0) # remove 'mmol' top level
    mmol_inchi_df = mmol_inchi_df.add_prefix('_raw_mmol_')
    mmol_inchi_df.fillna(value=0, inplace=True, axis=1)

    molarity_inchi_df = amounts_df.pivot_table(index=['name'],
                                           values=['molarity'],
                                           columns=['inchikey'],
                                           aggfunc='sum')
    molarity_inchi_df.columns = molarity_inchi_df.columns.droplevel(0) # remove 'molarity' top level
    molarity_inchi_df = molarity_inchi_df.add_prefix('_raw_molarity_')
    molarity_inchi_df.fillna(value=0, inplace=True, axis=1)

    # add new targets as validated through pipeline and prepared
    # Should ideally have runid_vial ('name') as the index 
    additional_default_dfs = [mmol_inchi_df,
                              molarity_inchi_df,
                              sumbytype_molarity_df,
                              feats_df,
                              ratios_df,
                              calcs_df]

    escalate_final_df = report_df
    escalate_final_df.set_index('name', drop=True, inplace=True)
    for num, dataframe in enumerate(additional_default_dfs):
        try:
            dataframe.set_index('name', drop=True, inplace=True)
            modlog.info(f'{num} in additional dataframes reindexed by runid_vial')
        except KeyError:
            modlog.info(f'{num} in additional dataframes already correctly indexed')
        escalate_final_df = escalate_final_df.join(dataframe)
    escalate_final_df.drop_duplicates(keep='first', inplace=True)
    final_df = cleaner(escalate_final_df, raw_bool) 
    start_count = final_df.shape[1]
    # Remove all columns that are entirely '0' or 'null'
    # Even if all the values are ACTUALLY 0, there is no variance, wgaf?
    condition_1 = (final_df == 0).all()
    condition_2 = (final_df.astype(str) == 'null').all()
    final_df = final_df.loc[:, ~condition_1]
    final_df = final_df.loc[:, ~condition_2]
    end_count = final_df.shape[1]

    modlog.info(f'Removed {start_count-end_count} of an original {start_count} columns which contained only "0" or "null"')
    print(f'Removed {start_count-end_count} of an original {start_count} columns which contained only "0" or "null"')
    modlog.info('successfully generated mmol and molarity dataframes for calcs')
    # TODO: cleanup documentation and export pipeline for statesets
    # TODO: create final export of a 2d CSV file from the data above
    return final_df

