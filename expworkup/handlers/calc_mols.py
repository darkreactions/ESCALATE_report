import pandas as pd
import numpy as np
import logging

from utils.globals import compound_ingredient_chemical_return

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

def get_mmol_df(reagent_volumes_df, 
                object_df, 
                chemical_count, 
                conc_model='default_conc'):
    """ returns the calculated mmol of each chemical 
     options defined by expworkup'solud_conc', 'solv_conc', default_conc]):
    """
    mmol_df = pd.DataFrame()

    modlog.info("mmol calculations and df creation")
    for reagent in reagent_volumes_df.columns:
        new_column_list = []
        convert_name = (reagent.rsplit('_', 1)[0].split('_', 1)[1]) #_raw_reagent_0_volume to raw_reagent_0
        new_column_list.extend([f'_{convert_name}_chemicals_{i}_mmol' for i in range(chemical_count)])

        #for each reagent, gather the concentrations of the associated chemicals in each reagent
        conc_df_temp = \
            object_df.loc[:, convert_name].apply(lambda x: 
                                           compound_ingredient_chemical_return(x, 
                                                                               chemical_count, 
                                                                               conc_model))

        # (M / L  * volume (uL) * (1L / 1000mL) * (1mL / 1000uL) * (1000mmol / 1mol) = mmol 
        mmol_df_temp = \
            conc_df_temp.loc[:,].multiply(reagent_volumes_df[reagent], 
                                          axis='index') / 1000

        mmol_df_temp.columns = new_column_list
        #possible TODO: add validation using the inchikey reads from the report_df
        mmol_df = mmol_df.join(mmol_df_temp, how='outer')
    modlog.info("Completed: 'mmol calculations and df creation'")
    return mmol_df