import logging
import pandas as pd

from tqdm import tqdm
from utils.file_handling import get_experimental_run_lab
from utils.file_handling import write_debug_file
#This should be the default in pandas IMO
pd.options.mode.chained_assignment = None

from expworkup.ingredients.compound_ingredient import CompoundIngredient

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def ingredient_pipeline(report_df, chemdf_dict, debug_bool):
    """
    This needs to read in the experiments, chemicals and assemble all unique reagents

    Parameters
    ----------
    report_df : pandas.DataFrame containing all parsed experiments
        originates from the expworkup.createjson --> expworkup.jsontocsv pipeline
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction

    Returns
    -------
    compound_ingredient_objects_df : pd.DataFrame of CompoundIngredient objects
        objects = are instances of chemical combinations generated in the lab.
        each reagent is rendered to a CompoundIngredient object and report to 
        the dataframe.  

    TODO:currently disabled... 
    compound_ingredient_models_df : pd.DataFrame of CompoundIngredient models
        models are target descriptions (nominals)
        each reagent specified in an experiment is rendered to a CompoundIngredient
        object and report to the dataframe.  Every experiment is explicitly described
        in terms of the reagents

    Notes
    --------
        * the difference between objects (e.g., actuals) and models (e.g., nominals) 
          indicates how well a hyptothesis was executed
    """
    report_copy = report_df.copy().set_index('name')
    
    #TODO: Fix to only implement a new CompoundIngredient as needed (currently implements for all instances)
    ingredients_actual_df = get_ingredients_df(report_df, nominal=False)# default is to return the actuals (nominals can be toggled)
    ingredients_actual_df['name'] = report_df['name']
    ingredients_actual_df_copy = ingredients_actual_df.set_index('name')
    reagent_volumes_df = report_copy.filter(regex='reagent_._volume')
    temperature_df = report_copy.filter(regex='temperature')
    measures_df = reagent_volumes_df.join(ingredients_actual_df_copy, how='left', on='name')
    measures_df = measures_df.join(temperature_df, how='left', on='name')
    if debug_bool:
        measures_df_file = 'REPORT_MEASURES.csv'
        # converts to 'name' indexed and unpacks with each inchikey as a column
        write_debug_file(measures_df.sort_index(axis=1),
                         measures_df_file)

    compound_ingredient_objects_df = get_compound_ingredient_objects_df(ingredients_actual_df, chemdf_dict)
    # TODO: export reagent objects/actuals df with concentrations, recipes, etc

    #TODO: repeat the object process but use the nominal values and return the models
    #ingredients_nominals_df = get_ingredients_df(report_df, nominal=True)# default is to return the actuals (nominals can be toggled)
    #ingredients_nominals_df['name'] = report_df['name']
    #compound_ingredient_models_df = get_compound_ingredient_objects_df(ingredients_actual_df, chemdf_dict)
    # TODO: export reagent models df with concentrations, recipes, etc

    return(compound_ingredient_objects_df)#, compound_ingredient_models_df)

def get_ingredients_df(report_df, nominal=False):
    '''
    gather up all reagent preparation infomation and return df of 
    reagent entities indexed by uid

    :param perovskite_df: dataframe rendered by escalate_report v0.8.1
    :param nominal: generated the dataframe using the nominal amounts (default uses actuals)

    :return: dataframe of all unique reagents used in the campaign (all unique
    from the dataset perovskite_df)
    '''
    # Len of all_chem_inchis should be reagents *max chemical from report (e.g. 9 reagents * 4 chemicals = 36)
    selected_columns = []

    all_chem_inchis = [x for x in report_df.columns if '_raw_reagent_' in x \
                  and 'chemicals' in x \
                  and 'inchikey' in x]
    selected_columns.extend(all_chem_inchis)

    # reads in all of the recorded volume observations (SOlution observations)
    recorded_volumes = [x for x in report_df.columns if '_raw_reagent_' in x \
                        and 'instructions' in x \
                        and 'volume' in x]
    selected_columns.extend(recorded_volumes)

    all_amounts =  [x for x in report_df.columns if '_raw_reagent_' in x \
                    and 'chemicals' in x \
                    and 'amount' in x]

    if nominal == True:
        # down select to nominals
        all_amounts_curated = [x for x in all_amounts if 'nominal' in x]
        selected_columns.extend(all_amounts_curated)
    else:
        # gets what was actually done
        all_amounts_curated = [x for x in all_amounts if 'actual' in x]
        selected_columns.extend(all_amounts_curated)

    reagent_details_df = report_df #perovskite_df[perovskite_df['name'].isin(representative_tray_uids)]
    reagent_details_df = reagent_details_df[selected_columns]
    return reagent_details_df

def one_compound_ingredient(one_ingredient_series_static, compound_ingredient_label, chemdf_dict):
    """
    Parameters
    ----------
    ingredient_series :
          ex. as generated by expworkup.ingredients.get_ingredients_df()
          _raw_reagent_2_chemicals_0_inchikey           VAWHFUNJDMQUSB-UHFFFAOYSA-N
          _raw_reagent_2_chemicals_1_inchikey           ZMXDDKWLCZADIW-UHFFFAOYSA-N
          _raw_reagent_2_instructions_2_volume                                  5.4
          _raw_reagent_2_instructions_2_volume_units                     milliliter
          _raw_reagent_2_chemicals_0_actual_amount                             2.16
          _raw_reagent_2_chemicals_0_actual_amount_units                       gram
          _raw_reagent_2_chemicals_1_actual_amount                              3.5
          _raw_reagent_2_chemicals_1_actual_amount_units                 milliliter
          name                              2020-01-23T18_13_57.034292+00_00_LBL_C9

          This can also include more or less chemicals.  MUST include
          an amount (nominal or actual), units for each amount, and inchikey
          identifiers.  Identifiers must be included in the associated chem_df
          of the run (i.e., LBL inventory in the example above)
          The series labels should be correctly associated by chemical label where
          "chemical_0" through "chemical_{n}" where n is the total number of 
          chemicals
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction.  
        The content of the inventory must meet the minimum requirements described in:
        https://github.com/darkreactions/ESCALATE_Capture/blob/master/capture/DataStructures_README.md

    Returns
    -----------
    compound_ingredient : class object of type ReagentObject 
        Object with all of the properties as described by
        expworkup.ingredients.compound_ingredient.ReagentObject

    """
    one_ingredient_series = one_ingredient_series_static.copy()
    experiment_uid = one_ingredient_series.pop('name')
    compound_ingredient_label_uid = experiment_uid + '_' + compound_ingredient_label.split('_', 1)[1]

    experiment_lab = get_experimental_run_lab(experiment_uid.rsplit('_', 1)[0])

    chem_df = chemdf_dict[experiment_lab]#.set_index('InChI Key (ID)')

    if one_ingredient_series.isnull().all():
        return(None)
    else:
        one_ingredient_series.dropna(inplace=True)
        myreagent_object = CompoundIngredient(one_ingredient_series,
                                              compound_ingredient_label_uid,
                                              chem_df)
        return(myreagent_object)

def get_compound_ingredient_objects_df(all_ingredients_df, chemdf_dict):
    """Generate CompoundIngredient (reagents) objects for all experiments

    Builds a pd.DataFrame which fully enumerates the contents of each
    CompoundIngredient (reagent) for each experiment across the 
    specified dataset(s)

    Parameters
    ----------
    all_ingredients_df : pd.DataFrame with full reagent description
        generated from get_ingredients_df() from the report_df
    
    chemdf_dict : dict of pandas.DataFrames assembled from all lab inventories
        reads in all of the chemical inventories which describe the chemical content
        from each lab used across the dataset construction.  
        The content of the inventory must meet the minimum requirements described in:
        https://github.com/darkreactions/ESCALATE_Capture/blob/master/capture/DataStructures_README.md

    Returns
    -------
    compound_ingredient_objects_df : pd.DataFrame of CompoundIngredient objects
        each reagent specified in an experiment is rendered to a CompoundIngredient
        object and report to the dataframe.  Every experiment is explicitly described
        in terms of the reagents.
    """

    compound_ingredient_objects_df = pd.DataFrame()

    compound_ingredient_list = []
    for column in all_ingredients_df.columns:
        if "_raw_reagent_" in column:
            ingredient_instance = '_'.join(column.split('_')[1:4])
            ingredient_instance = ingredient_instance
            compound_ingredient_list.append(ingredient_instance)
    compound_ingredient_list = set(compound_ingredient_list)

    modlog.info('Preparing Reagent Objects, details for ingredient preparation are in separate logfile')
    print('(4/6) Preparing reagent objects... (this is slow on large datasets)')
    for compound_ingredient_label in tqdm(compound_ingredient_list):
        modlog.info(f'Preparing {compound_ingredient_label}')
        df = all_ingredients_df.filter(regex=compound_ingredient_label)
        df['name'] = all_ingredients_df['name'].values
        # TODO: parallelize this for performance increase
        # https://stackoverflow.com/questions/36794433/python-using-multiprocessing-on-a-pandas-dataframe
        compound_ingredient_objects_df[compound_ingredient_label] = \
            df.apply(lambda row:  one_compound_ingredient(row,
                                                          compound_ingredient_label,
                                                          chemdf_dict),
                                                          axis=1)
        modlog.info(f'Completed {compound_ingredient_label} reagent objects')
    modlog.info("Completed: 'Preparing Reagent Objects'")
    compound_ingredient_objects_df['name'] = df['name'].values
    compound_ingredient_objects_df.set_index('name', inplace=True)
    return(compound_ingredient_objects_df)

