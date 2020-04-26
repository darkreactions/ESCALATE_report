import logging
import pandas as pd

from utils.file_handling import write_debug_file
from expworkup.handlers.cleaner import cleaner
from expworkup.handlers.chemical_types import get_unique_chemicals_types_byinstance

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def construct_2d_view(report_df, 
                      calc_out_df,
                      runUID_inchi_file,
                      inchi_key_indexed_features_df,
                      debug_bool,
                      raw_bool):
    """ Combines the generated dataframes into a single 2d csv for export

    
    """
    modlog.info("Generating 2d dataframe")
    print(f'Exporting 2d Dataframe...')

    # Some final exports for ETL in V3
    res = calc_out_df.pivot_table(index=['name','inchikey'], 
                                  values=['M'],
                                  columns=['main_type'],
                                  aggfunc='sum')

    res.columns = res.columns.droplevel(0) # remove Molarity top level
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
    mmol_inchi_df = calc_out_df.pivot_table(index=['name'],
                                           values=['mmol'],
                                           columns=['inchikey'],
                                           aggfunc='sum')
    mmol_inchi_df.columns = mmol_inchi_df.columns.droplevel(0) # remove 'mmol' top level
    mmol_inchi_df = mmol_inchi_df.add_prefix('_raw_mmol_')
    mmol_inchi_df.fillna(value=0, inplace=True, axis=1)

    molarity_inchi_df = calc_out_df.pivot_table(index=['name'],
                                           values=['M'],
                                           columns=['inchikey'],
                                           aggfunc='sum')
    molarity_inchi_df.columns = molarity_inchi_df.columns.droplevel(0) # remove 'M' top level
    molarity_inchi_df = molarity_inchi_df.add_prefix('_raw_molarity_')
    molarity_inchi_df.fillna(value=0, inplace=True, axis=1)

    # add new targets as validated through pipeline and prepared
    # Should ideally have runid_vial as the index by this point...
    additional_default_dfs = [mmol_inchi_df,
                              molarity_inchi_df,
                              sumbytype_molarity_df,
                              feats_df]

    escalate_final_df = report_df
    escalate_final_df.set_index('name', drop=True, inplace=True)
    for num, dataframe in enumerate(additional_default_dfs):
        try:
            dataframe.set_index('name', drop=True, inplace=True)
            modlog.info(f'{num} in additional dataframes reindexed by runid_vial')
        except KeyError:
            modlog.info(f'{num} in additional dataframes already correctly indexed')
        escalate_final_df = escalate_final_df.join(dataframe)
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


def runuid_feat_merge(sumbytype_byinstance_molarity_df, inchi_key_indexed_features_df):
    """ Merge and rename function for runuid + report_feats dataframes


    Notes
    ------
    Currently set to rename smiles, standardized smiles, and type information
    to _raw_ section of the dataframe.  

    """
    chemical_type_inchi = \
        sumbytype_byinstance_molarity_df.filter(regex='_inchikey')
    for type_inchi_col in chemical_type_inchi.columns:
        chemical_type = type_inchi_col.split('_')[2].strip() # _raw_inorganic_0_inchikey to inorganic
        feature_prefix = type_inchi_col.rsplit('_', 1)[0].strip().split('_',2)[2] # _raw_inorganic_0_inchikey to inorganic_0
        bulk_features = inchi_key_indexed_features_df.copy()
        bulk_features = bulk_features[bulk_features['types'].str.contains(pat=f'(?:^|\W){chemical_type}(?:$|\W)', regex=True)]
        #Drop anycolumns which are not full (likely due to specifying multiple types)
        bulk_features.dropna(axis=1, how='any', inplace=True)
        #Rename columns to fit with ESCALATE naming scheme (Brute force, not elegant)
        column_rename = {}
        drop_list = []
        known_drops = ['smiles_standardized', 'smiles', 'types']
        for column in bulk_features.columns:
            if any(column == known_drop for known_drop in known_drops):
                column_rename[column] = column
                drop_list.append(column)
            elif 'feat' in column:
                newcolumnname = column.split('_', 2)[2]
                column_rename[column] = newcolumnname
        bulk_features.rename(columns=column_rename, inplace=True)
        bulk_features.drop(drop_list, inplace=True, axis=1)
        bulk_features = bulk_features.add_prefix(f'_feat_{feature_prefix}_')
        chemical_type_inchi = chemical_type_inchi.join(bulk_features, on=type_inchi_col, rsuffix='DROPME_AFTER_MERGE')
    chemical_type_inchi.dropna(axis=1, how='all', inplace=True)
    final_drop_list = chemical_type_inchi.filter(regex='DROPME_AFTER_MERGE').columns
    chemical_type_inchi.drop(final_drop_list, inplace=True, axis=1)
    return chemical_type_inchi