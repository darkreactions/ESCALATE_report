
# expand the set of unique inchis into its own dataframe

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