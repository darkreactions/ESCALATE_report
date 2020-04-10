import logging

modlog = logging.getLogger('report.calc_molarity')
import pandas as pd

def get_molarity_df(mmol_df):
        # (M / L  * volume (uL) * (1L / 1000mL) * (1mL / 1000uL) * (1000mmol / 1mol) = mmol 
        mmol_df_temp2 = mmol_df.multiply(reagent_volumes_df[reagent], axis='index') / 1000 