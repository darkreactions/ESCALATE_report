import pandas as pd
import numpy as np
import logging

from utils.globals import compound_ingredient_chemical_return

modlog = logging.getLogger('report.calc_mols')

def get_mmol_df(reagent_volumes_df, 
                object_df, 
                chemical_count, 
                conc_model='default_conc'):
    """ returns the calculated mmol of each chemical 
    , 'solud_conc', 'solv_conc']):
    """
    mmol_df = pd.DataFrame()

    for reagent in reagent_volumes_df.columns:
        new_column_list = []
        mmol_df_temp = pd.DataFrame
        convert_name = (reagent.rsplit('_', 1)[0].split('_', 1)[1]) #_raw_reagent_0_volume to raw_reagent_0
        new_column_list.extend([f'_{convert_name}_chemicals_{i}_mmol' for i in range(chemical_count)])

        #for each reagent, gather the concentrations of the associated chemicals in each reagent
        conc_df_temp = \
            object_df.loc[:, convert_name].apply(lambda x: 
                                           compound_ingredient_chemical_return(x, 
                                                                               chemical_count, 
                                                                               conc_model))
    return mmol_df
        #TODO: calculate mmols of each compound
#        mmol_df_temp.columns = new_column_list
        #possible TODO: add validation using the inchikey reads from the report_df
            #can be a repeat of the mmol_df_temp above just targeting a different func
#        mmol_df = mmol_df.join(mmol_df_temp, how='outer')
#    return(mmol_df)
