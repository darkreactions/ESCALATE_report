#Copyright (c) 2020 Ian Pendleton - MIT License

import logging
import pandas as pd


modlog = logging.getLogger(__name__)

def feat_pipeline(target_name, report_df, calc_df, chem_df_dict):
    """
    """
    feat_df = pd.DataFrame()

    modlog.info(f'Generating physicochemical features to {target_name} dataset')
    print(f'Generating physicochemical features to {target_name} dataset')

    return feat_df