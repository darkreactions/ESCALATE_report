#Copyright (c) 2020 Ian Pendleton - MIT License
import logging
import pandas as pd

from utils.file_handling import write_debug_file
from expworkup.handlers.chemical_types import get_chemical_types
from expworkup.handlers import calc_mols
from expworkup.handlers import inchigen

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')



def calc_pipeline(report_df, object_df, chemdf_dict, debug_bool):
    """ Ingest pipeline which handles offloading of _calc_ functions

    #TODO: update this docstring
    Parameters
    ----------
    report_df : pandas.DataFrame
        dataframe returned after parsing all content from google drive
        returned from expworkup.json_pipeline
    
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
    molarity_df.sort_index(inplace=True) ##**
    res = molarity_df.pivot_table(index=['name','inchikey'], 
                                  values=['M'],
                                  columns=['main_type'],
                                  aggfunc='sum')

    res.columns = res.columns.droplevel(0) # remove Molarity top level
    sumbytype_molarity_df = res.groupby(level=0).sum()  ##**
    if debug_bool:
        sumbytype_molarity_df_file = 'REPORT_MOLARITY_BYTYPE_CALCS.csv'
        write_debug_file(sumbytype_molarity_df, sumbytype_molarity_df_file)

    sumbytype_byinstance_molarity_df = get_unique_chemicals_types_byinstance(res) ##**
    if debug_bool:
        sumbytype_byinstance_molarity_df_file = \
            'REPORT_MOLARITY_BYTYPE_BYINSTANCE_CALCS.csv'
        write_debug_file(sumbytype_byinstance_molarity_df, 
                         sumbytype_byinstance_molarity_df_file)
    modlog.info('successfully generated mmol and molarity dataframes for calcs')

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
    return sumbytype_molarity_df

def expand_columns(df, column):
    """ expands specified column with lists into multiple columns
    """
    df = pd.DataFrame(df[column].values.tolist(), df.index).add_prefix(f'{column}_')
    return df

def chemical_type_sorting(x):
    """ parse the lines of the molarity table to compress type values to a single column of lists
    """
    molarity_list = []
    temp_id = []
    for column in x.columns:
        raw_M_list = x[column].values.tolist()
        cleanedlist = [[i, conc] for i, conc in enumerate(raw_M_list) if str(conc) != 'nan']
#        cleanedlist = [x for x in raw_M_list if str(x) != 'nan']
        molarity_list.append([element[1] for element in cleanedlist])
        temp_id.append([x.index[element[0]][1] for element in cleanedlist])
    molarity_list.extend(temp_id)
    sorted_dict = {x.index[0][0] : molarity_list}
    mycolumns = x.columns.tolist()
    mycolumns.extend(f'{colname}_inchikey' for colname in x.columns)
    sorted_df = pd.DataFrame.from_dict(sorted_dict,
                                       orient='index',
                                       columns=mycolumns)
    return(sorted_df)

def get_unique_chemicals_types_byinstance(molarity_df_type_pivot):
    """ Unique handler to gather unique main_type instances into individual columns
      molarity column and inchi column as list of lists

    TODO: add to schemas.py
    Parameters
    ----------


    Returns
    -------
    unique_chemicals_types_byinstance : pd. DataFrame
        include the [M]olarity of each unique chemical type in a given run
        along with the inchikey of the particular chemical. 
        Headers are consistent
        Ex.
                          acid_0  inorganic_0 inorganic_1 ... solvent_inchikey_0 
        name                                                              
        2017-10 ... _A1  2.923979 0.662557     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _B1  4.779492 0.610422     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _C1  4.779492 0.610422     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _D1  4.194307 0.626864     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N
        2017-10 ... _E1  3.576726 0.644217     NaN  YEJRWHAVMIAJKC-UHFFFAOYSA-N

    """
    chemicals_types_bundle_df = \
        molarity_df_type_pivot.groupby(level=[0]).apply(lambda x: chemical_type_sorting(x))
    chemicals_types_bundle_df = chemicals_types_bundle_df.droplevel(1) #remove extra runUID column
    #unapck the bundle 
    unique_chemicals_types_byinstance = \
        pd.DataFrame(index=chemicals_types_bundle_df.index)
    for column in chemicals_types_bundle_df.columns:
        mynew_df = expand_columns(chemicals_types_bundle_df, column)
        unique_chemicals_types_byinstance = \
            unique_chemicals_types_byinstance.join(mynew_df, how='left')
    return unique_chemicals_types_byinstance

def _compute_proportional_conc(perovRow, v1=True, chemtype='organic'):
    """Compute the concentration of acid, inorganic, or acid for a given row of a crank dataset
    icky icky, don't use quickly!
    
    Intended to be pd.DataFrame.applied over the rows of a crank dataset
    
    :param perovRow: a row of the crank dataset
    :param v1: use v1 concentration or v0 
    :param chemtype: in ['organic', 'inorganic', 'acid']
    
    
    Currently hard codes inorganic as PbI2 and acid as FAH. TODO: generalize
    """
    inchis = {
        'organic': perovRow['_rxn_organic-inchikey'],
        # Inorganic assumes PbI2, so far the only inorganic in the dataset
        'inorganic': 'RQQRAHKHDFPBMC-UHFFFAOYSA-L',
        ## acid assumes FAH (as of writing this the only one in the dataset)
        'acid': 'BDAGIHXWWSANSR-UHFFFAOYSA-N'
    }
    speciesExperimentConc = perovRow[f"{'_rxn_M_' if v1 else '_rxn_v0-M_'}{chemtype}"]
    
    reagentConcPattern = f"_raw_reagent_[0-9]_{'v1-' if v1 else ''}conc_{inchis[chemtype]}"
    speciesReagentConc = perovRow.filter(regex=reagentConcPattern)
    
    if speciesExperimentConc == 0: 
        return speciesExperimentConc
    else: 
        return speciesExperimentConc / np.max(speciesReagentConc)

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
        # 'unstack' converts to 'name' (runUID) indexed and unpacks with each inchikey as a column
        write_debug_file(summed_mmol_df.unstack().fillna(value=0), 
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