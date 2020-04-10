#Copyright (c) 2020 Ian Pendleton - MIT License
import logging
import pandas as pd

from expworkup.handlers import calc_mols
from expworkup.handlers import calc_molarity
from expworkup.handlers import inchigen

modlog = logging.getLogger('report.calcs')

def calc_pipeline(report_df, object_df, target_name, debug_bool):
    """ Ingest pipeline which handles offloading of _calc_ functions

    Parameters
    ----------
    target_name : name of the target folder (and final curated csv output)

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

    modlog.info(f'Starting calc_pipeline on {target_name}')

    inchi_df = report_copy.filter(regex='reagent_._chemicals_._inchikey')
    one_reagent_inchi_df = inchi_df.filter(regex='reagent_0_chemicals_._inchikey')
    chemical_count = one_reagent_inchi_df.shape[1]
    
    reagent_volumes_df = report_copy.filter(regex='reagent_._volume')

    default_mmol_df_nums = calc_mols.get_mmol_df(reagent_volumes_df, 
                                                 object_df,
                                                 chemical_count,
                                                 conc_model='default_conc')
    
    #The order of chemicals (0,1,etc) is maintained in CompoundIngredients
    # we can assume that one describes the contents of the other directly
    
    #default_mmol_df = default_mmol_df_nums.join(inchi_df, 
    #                                            how='outer')

    modlog.info('augmented dataframe with chemical calculations (concentrations)')
    print(f'Appending physicochemical features to {target_name} dataframe')

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