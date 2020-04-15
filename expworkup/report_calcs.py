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

    default_mmol_df_nums = calc_mols.get_mmol_df(reagent_volumes_df, 
                                                 object_df,
                                                 chemical_count,
                                                 conc_model='default_conc')

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
    # arrange with each reagent+chemical combination as a row and [inchikey+mmol] as columns
    stacked_mmol_df = labeled_mmol_df.stack(0)
    # Sum mmol columns where the inchikey matches 
    # has row numbers = uniqueinchis * unique runUIDs  (only values are mmols)
    # sadly, there is no way to store 'units' in the pandas metadata
    # so we use the first level index to store it, see example of access below
    summed_mmol_series = stacked_mmol_df.groupby(['name','inchikey'])['mmol'].sum()
    summed_mmol_series = \
        summed_mmol_series.drop(summed_mmol_series[summed_mmol_series == 0].index)
    summed_mmol_df = summed_mmol_series.to_frame(name='mmol')
    if debug_bool:
        summed_mmol_df_file = 'REPORT_MMOL_CALCS.csv'
        # 'unstack' converts to 'name' (runUID) indexed and unpacks with each inchikey as a column
        write_debug_file(summed_mmol_df,#.unstack().fillna(value=0), 
                         summed_mmol_df_file)

    ##### Calc functions begin here ####
    if 
    #(mmol / uL)*(1mol/1000mmol)*(1000uL/1mL)*(1000mL/1L)=mol/L=[M]
    molarity_series = \
        summed_mmol_series / reagent_volumes_df.sum(axis=1) * 1000
    molarity_df = molarity_series.to_frame(name='M')
    if debug_bool:
        molarity_df_file = 'REPORT_MOLARITY_CALCS.csv'
        # 'unstack' converts to 'name' (runUID) indexed and unpacks with each inchikey as a column
        write_debug_file(molarity_df,#.unstack().fillna(value=0), 
                         molarity_df_file)

    molarity_df = molarity_df.join(report_copy['_raw_lab'], how='left', on='name')

    # add types to molarity dataframe (useful for ratios) 
    # This shows an example of multiindex use, will be used in mol ratios
    molarity_df[['types','smiles']] = \
        molarity_df.apply(lambda x: get_chemical_types(x.name[1],#inchikesy 
                                                       x['_raw_lab'],
                                                       chemdf_dict),
                                                              axis=1)
    # Calculate concentration ratio of all types (a:b and b:a, both needed for ML)
    # Categorize each of the concentrations by the first type and sum repetition to runUID
    molarity_df.loc[:, 'main_type'] = molarity_df.types.map(lambda x: x[0])
    res = molarity_df.pivot_table(index=['name','inchikey'], 
                                  values='M',
                                  columns=['main_type'],
                                  aggfunc='sum')
    print(res.groupby(level=[0]).sum())

#    old_idx = res.index.names
#    print(old_idx)
#    my_new_df = res.reset_index().dropna(axis=0).set_index(old_idx)

#    print(res.index.dropna(axis=0).droplevel(level=1).reindex())
#    print(res.droplevel(level=1).reindex())






    """
    ## Export all of the dataframes  with columns "name", in, alt-in, out, command, version
    ##  the name of the dataframe on export should be _calc_<command>.csv
    ##  append the command, version, and header of all of the in and out columns to the command-type table
    ##  exported at the end of this code 
    """

    #raw_molarity_clean = nameCleaner(clean_df.filter(like='_raw_M_'), '_raw_v0-M')

    #solv_mmol_df = calc_mols.get_mmol_df(reagent_volumes_df, object_df, chemical_count, conc_model='default_conc')
    #Gets the mmol of each CHEMICAL and returns them summed and uniquely indexed
    #mmol_df=calcmmol.mmol_breakoff(tray_df, runID_df)
    #augmented_raw_df = augmentdataset(report_df)
    #rxn_v1molarity_clean = nameCleaner(clean_df.filter(like='_raw_v1-M_'), '_rxn_M')
    #fullConc_df=(concentration_df.append([concentration_df]*wellcount,ignore_index=True))

#    return(calc_df)

def _compute_proportional_conc(perovRow, v1=True, chemtype='organic'):
    """Compute the concentration of acid, inorganic, or acid for a given row of a crank dataset
    TODO: most of this works, just need to test before deploy
    
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