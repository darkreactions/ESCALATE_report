#Copyright (c) 2020 Ian Pendleton - MIT License
import logging
import pandas as pd

from expworkup.handlers import calc_mols
from expworkup.handlers import calc_molarity
from expworkup.handlers import inchigen

modlog = logging.getLogger(__name__)

def calc_pipeline(report_df, object_df, target_name, debug_bool):
    """ Ingest pipeline which handles offloading of _calc_ functions

    Parameters
    ----------
    target_name : name of the target folder (and final curated csv output)
    report_df : pandas.DataFrame
        dataframe returned after parsing all content from google drive

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
    print(f'Appending physicochemical features to {target_name} dataframe')

#    return(calc_df)

def nameCleaner(sub_dirty_df, new_prefix):
    ''' The name cleaner is hard coded at the moment for the chemicals
    we are using at HC/ LBL
    TODO: Generalize name cleaner for groups or "m_types" based on inchikey
    or chemical abbreviation

    '''
    organic_df = pd.DataFrame()
    cleaned_M = pd.DataFrame()
    for header in sub_dirty_df.columns:
        # m_type = solvent (all solvent category data)
        if 'YEJRWHAVMIAJKC-UHFFFAOYSA-N' in header \
                or 'ZMXDDKWLCZADIW-UHFFFAOYSA-N' in header \
                or 'IAZDPXIOMUYVGZ-UHFFFAOYSA-N' in header \
                or 'YMWUJEATGCHHMB-UHFFFAOYSA-N' in header \
                or 'ZASWJUOMEGBQCQ-UHFFFAOYSA-L' in header \
                or 'UserDefinedSolvent' in header:  # This one is PbBr2 (just need to pass for now!)
            pass
        # m_type = acid
        elif "BDAGIHXWWSANSR-UHFFFAOYSA-N" in header:
            cleaned_M['%s_acid' % new_prefix] = sub_dirty_df[header]
        # m_type = inorganic (category of "inorgnic" used for HC/ LBL)
        elif 'RQQRAHKHDFPBMC-UHFFFAOYSA-L' in header:
            cleaned_M['%s_inorganic' % new_prefix] = sub_dirty_df[header]
        else:
            organic_df[header] = sub_dirty_df[header]
    cleaned_M['%s_organic' % new_prefix] = organic_df.sum(axis=1)
    return(cleaned_M)

def augmentdataset(raw_df):
    ''' Processes full dataset through a series of operations to add molarity, features, calculated values, etc

    Takes the raw dataset compiled from the JSON files of each experiment and 
    performs rudimentary operations including: calculating concentrations and
    adding features.

    *This needs to be broken out into a separate module with each task allocated
    a single script which can be edited independently
    '''
    rawdataset_df_filled = raw_df.fillna(0)  #ensures that all values are filled (possibly problematic as 0 has a value)
    dataset_calcs_fill_df = augmolarity(rawdataset_df_filled) 
    dataset_calcs_desc_fill_df = augdescriptors(dataset_calcs_fill_df)
    return(dataset_calcs_desc_fill_df)

def augmolarity(concat_df_final):
    ''' Perform exp object molarity calculations (ideal concentrations), grab organic inchi
    
    grabs all of the raw mmol data from the column header and creates a column which uniquely 
    identifies which organic will be needed for the features in the next step
    '''
    concat_df_final.set_index('RunID_vial', inplace=True)
    inchi_df = concat_df_final.filter(like='_InChIKey')

    #takes all of the volume data from the robot run and reduces it into two total volumes, the total prior to FAH and the total after.  Returns a 3 column array "totalvol and finalvol in title"
    molarity_df=calcmolarity.molarity_calc(concat_df_final, 2) #2 is the hardcoded FAH final count

    #Sends off the final mmol list to specifically grab the organic inchi key and expose(current version)
    OrganicInchi_df=inchigen.GrabOrganicInchi(inchi_df, molarity_df)
    
    #Combines the new Organic inchi file and the sum volume with the main dataframe
    dataset_calcs_fill_df=pd.concat([OrganicInchi_df, concat_df_final, molarity_df], axis=1, join_axes=[concat_df_final.index])
    return(dataset_calcs_fill_df)

def augdescriptors(dataset_calcs_fill_df):
    ''' bring in the inchi key based features for a left merge

    Temporary holder for processing the descriptors and adding them to the complete dataset.  
    If an amine is not present in the "perov_desc.csv1" file, the run will not be processed
    and will error out silently!  This is a feature not a bug (for now)  
    '''
    with open('data/perov_desc_edited.csv', 'r') as my_descriptors:
       descriptor_df=pd.read_csv(my_descriptors) 
    dirty_full_df=dataset_calcs_fill_df.merge(descriptor_df, left_on='_rxn_organic-inchikey', right_on='_raw_inchikey', how='inner')
    runID_df_big=pd.DataFrame(data=[dirty_full_df['_raw_jobserial'] + '_' + dirty_full_df['_raw_vialsite']]).transpose()
    runID_df_big.columns=['RunID_vial']
    dirty_full_df=pd.concat([runID_df_big, dirty_full_df], axis=1)
    dirty_full_df.set_index('RunID_vial', inplace=True)
    my_descriptors.close()
    return(dirty_full_df)


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