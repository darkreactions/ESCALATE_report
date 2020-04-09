import pandas as pd 

def get_tray_uids(perovskite_df):
    '''
    inputs the perovskite dataset and returns a list of UIDs associated 
    with all unique compound ingredient objects 

    :param perovskite_df: generated perovskite dataset using 
    ESCALATE_report v0.8.1

    :return: an instance of every unique set experiments in the dataset
    '''
    all_uids = perovskite_df['name'].tolist()

    tray_uids = []
    exp_tray_dict = {}
    for experiment_uid in all_uids:
        tray_uid = experiment_uid.rsplit('_', 1)[0]
        tray_uids.append(tray_uid)
        exp_tray_dict[tray_uid] = experiment_uid

    # TODO: select all unique instances based on the chemical content not UID 
    # Until then, do it the long way
    #tray_uids = set(tray_uids)
    #
    #representative_experiments = []
    #for tray_uid in tray_uids:
    #    representative_experiments.append(exp_tray_dict[tray_uid])

    return(all_uids)