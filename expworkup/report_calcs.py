#Copyright (c) 2020 Ian Pendleton - MIT License
import logging
import pandas as pd

from utils.file_handling import write_debug_file
from expworkup.handlers.chemical_types import get_chemical_types
from expworkup.handlers.chemical_types import get_unique_chemicals_types_byinstance
from expworkup.handlers import calc_mols

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def calc_pipeline(report_df, object_df, chemdf_dict, debug_bool):
    """ Ingest pipeline which handles offloading of _calc_ functions

    #TODO: update this docstring
    Parameters
    ----------
    report_df : pandas.DataFrame
        2d dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline
        
    compound_ingredient_objects_df : pd.DataFrame of CompoundIngredient objects
        objects = are instances of chemical combinations generated in the lab.
        each reagent is rendered to a CompoundIngredient
        object and report to the dataframe.  Every experiment is explicitly described
        in terms of the reagents
    
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction

    debug_bool : CLI argument, True=Enable debugging
        ETL export option mainl
    


    Returns
    ---------
    calc_df :  pandas.DataFrame of concatenated calculations
        does not include the report_df.  Includes, chemical types, 

    """
    report_copy = report_df.set_index('name')

    modlog.info(f'Starting calc_pipeline on target datasets')
    print(f'(6/6) ESCALATing dataset...')

    inchi_df = report_copy.filter(regex='reagent_._chemicals_._inchikey')
    one_reagent_inchi_df = inchi_df.filter(regex='reagent_0_chemicals_._inchikey')
    chemical_count = one_reagent_inchi_df.shape[1]
    reagent_volumes_df = report_copy.filter(regex='reagent_._volume')

    #TODO: Iterate pipeline from 'default_conc' to other i.e., experimental observation 
    default_mmol_df_nums = calc_mols.get_mmol_df(reagent_volumes_df, 
                                                 object_df,
                                                 chemical_count,
                                                 conc_model='default_conc')

    modlog.info('generating a dataframe of combined mmol and inchi data')
    stacked_mmol_df = get_summed_mmol_series(default_mmol_df_nums, 
                                                reagent_volumes_df, 
                                                inchi_df)
    summed_mmol_series = report_mmol(stacked_mmol_df, debug_bool)
    out_df = summed_mmol_series.copy()

    #(mmol / uL)*(1mol/1000mmol)*(1000uL/1mL)*(1000mL/1L)=mol/L=[M]
    summed_molarity_series = \
        summed_mmol_series / reagent_volumes_df.sum(axis=1) * 1000
    molarity_df = report_molarity(summed_molarity_series, debug_bool)

    modlog.info('applying chemical type information to each experiment')
    # add types to molarity dataframe (useful for ratios) 
    # This shows an example of multiindex use, will be used in mol ratios
    molarity_df = molarity_df.join(report_copy['_raw_lab'], how='left', on='name')
    molarity_df[['types','smiles']] = \
        molarity_df.apply(lambda x: get_chemical_types(x.name[1],#inchikesy 
                                                       x['_raw_lab'],
                                                       chemdf_dict),
                                                              axis=1)
    ### KEY DATAFRAMES marked with '##**'
    # Categorize each of the concentrations by the first type and sum repetition to runUID
    molarity_df.loc[:, 'main_type'] = molarity_df.types.map(lambda x: x[0])
    molarity_df.sort_index(inplace=True) 

    calc_ready_df = molarity_df.copy().join(out_df) ##**
    out_df = calc_ready_df.copy()
    res = calc_ready_df.pivot_table(index=['name','inchikey'], 
                                  values=['M'],
                                  columns=['main_type'],
                                  aggfunc='sum')

    res.columns = res.columns.droplevel(0) # remove Molarity top level
    sumbytype_molarity_df = res.groupby(level=0).sum()  ##*
    sumbytype_byinstance_molarity_df = get_unique_chemicals_types_byinstance(res) ##**

    """
    ## import type_command table, look for commands 
    ## Export all of the dataframes  with columns "name", in, alt-in, out, command, version
    ##  the name of the dataframe on export should be _calc_<command>.csv
    ##  append the command, version, and header of all of the in and out columns to the command-type table
    ##  exported at the end of this code 
    """
    ##### Calc functions begin here ####
    # TODO:see below
    #import requested functions  from command type
    # build helper functions for ratios
    # hanson solubility examples
    # build heleper fucntions for cxcalc interfacing (and examples)
    # same with rdkit
    # Calculate concentration ratio of specified types (a:b and b:a, both needed for ML)
    return out_df



def report_mmol(stacked_mmol_df, debug_bool):
    """ Cleans mmol data and exports dataframes of mmol amounts for ETL
    Parameters
    ----------
    stacked_mmol_df : pd.DataFrame with multiindex
        [runUID,_raw_reagent_._chemicals_.] multiindex as rows and [inchikey, mmol] as columns
        mmol values delineated reagent chemical combination are the values of each cell
        duplicate inchikeys are possible

    debug_bool :  CLI argument, True=Enable debugging
        ETL export option mainly

    Returns
    -------
    summed_mmol_series : pd.Series with ['name', 'inchikey'] multiindex
        values are the mmol of each runUID + inchikey combination

    Notes
    -----
    Sum mmol columns where the inchikey matches 
    has row numbers = uniqueinchis * unique runUIDs  (only values are mmols)
    sadly, there is no way to store 'units' in the pandas metadata
    """
    summed_mmol_series = stacked_mmol_df.groupby(['name','inchikey'])['mmol'].sum()
    summed_mmol_series = \
        summed_mmol_series.drop(summed_mmol_series[summed_mmol_series == 0].index)
    summed_mmol_df = summed_mmol_series.to_frame(name='mmol')
    if debug_bool:
        summed_mmol_df_file = 'REPORT_MMOL_CALCS.csv'
        summed_mmol_df_inchis_file = 'REPORT_MMOL_INCHICOLS_CALCS.csv'
        summed_mmol_df_unstacked = summed_mmol_df.unstack().fillna(value=0)
        summed_mmol_df_unstacked.columns = summed_mmol_df_unstacked.columns.droplevel(0) # remove repeating mmol top level
        # 'unstack' converts to 'name' (runUID) indexed and unpacks with each inchikey as a column
        write_debug_file(summed_mmol_df_unstacked, 
                         summed_mmol_df_inchis_file)
        write_debug_file(summed_mmol_df,#.unstack().fillna(value=0), 
                         summed_mmol_df_file)
    return summed_mmol_series 

def report_molarity(summed_molarity_series, debug_bool):
    """ Exports dataframes of molarity for ETL
    Parameters
    ----------
    summed_molarity_series : pd.Series with ['name', 'inchikey'] multiindex
        values are the Molarity of each runUID + inchikey combination

    debug_bool :  CLI argument, True=Enable debugging
        ETL export option mainly
    
    Returns
    -------
    molarity_df : pd.DataFrame with ['name', 'inchikey'] multiindex
        name is the runUID and inchikey is the identity of the chemical
        values of 'M' column are the calculated molarity based on molarity series
    """
    molarity_df = summed_molarity_series.to_frame(name='M')
    if debug_bool:
        molarity_df_file = 'REPORT_MOLARITY_CALCS.csv'
        molarity_df__inchi_file = 'REPORT_MOLARITY_INCHICOLS_CALCS.csv'
        # 'unstack' converts to 'name' (runUID) indexed and unpacks with each inchikey as a column
        write_debug_file(molarity_df.unstack().fillna(value=0),
                         molarity_df__inchi_file)
        write_debug_file(molarity_df,#.unstack().fillna(value=0), 
                         molarity_df_file)
    return molarity_df

def get_summed_mmol_series(default_mmol_df_nums, 
                           reagent_volumes_df,
                           inchi_df):
    """

    """
    #split _raw_reagent_._chemical_._inchikey to [_raw_reagent_._chemical_., inchikey] 
    idx = inchi_df.columns.str.rsplit('_', n=1, expand=True)
    inchi_df.columns = idx
    # index on _raw_reagent_._chemical_. and inchikey, respectively
    idx = pd.MultiIndex.from_product([idx.levels[0], idx.levels[1]])
    inchi_df.reindex(columns=idx, fill_value=-1)

    #split _raw_reagent_._chemical_._inchikey to [_raw_reagent_._chemical_., mmol] 
    idx = default_mmol_df_nums.columns.str.rsplit('_', n=1, expand=True)
    default_mmol_df_nums.columns = idx
    # index on _raw_reagent_._chemical_. and mmol, respectively
    idx = pd.MultiIndex.from_product([idx.levels[0], idx.levels[1]])
    default_mmol_df_nums.reindex(columns=idx, fill_value=-1)
 
    labeled_mmol_df = inchi_df.join(default_mmol_df_nums, how='left', on='name')
    # arrange with each runUID+chemical combination as a row and [inchikey+mmol] as columns
    stacked_mmol_df = labeled_mmol_df.stack(0)
    return stacked_mmol_df

