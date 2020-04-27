'''
script used for converting training and state set data into versioned data repo ready files

pepareexport function provides the entry point where the training, state, and link files should 
be provided. The script currently requires that the user specifies the 'crank' number (index_name variable).
The files should be present in the main directory of the ESCALATE_report code for this version.

The 'link' and state files are created using the ESCALATE_Capture code.  The "link" file should be named
XXX_link.csv where XXX is the name of the statespace without the .csv extension label.
'''

import logging
import pandas as pd
import csv
from datetime import datetime

TRAININGDATA_NAME = None
INDEX_NAME = None
STATESET_NAME = None
LINK_NAME = None

modlog = logging.getLogger(f'mainlog.{__name__}')
warnlog = logging.getLogger(f'warning.{__name__}')

### HEADER PREPARATION INFORMATION
## Header for state set data
# First run provided by Ian through the SD2E data repo
# 1553 runs, covering 6 different amines and 67 unique descriptors.
# Author: Ian Pendelton, Scott Novotney
# Index: first_run.index.csv 

## Header for training index
# Index of first run provided by Haverford through the SD2E TACC file system.
# Author: Scott Novotney, Ian Pendelton
# The name "first_run" is not significant.

## Header for state index
# Index of first run provided by Haverford through the SD2E TACC file system.
# Author: Scott Novotney, Ian Pendelton
# The name "first_run" is not significant


def trainheader(trainingfile, metdict):
    ''' generates metrics for training data header creation
    '''
    modlog.info('Building initial training text file containing header with metrics')
    with open(trainingfile, 'w') as t:
        print(f"#Training data generated on {datetime.utcnow()} for the perovskite dataset", file=t)
        print(f"#Index of {metdict['exp#']} experiments, covering {metdict['featcount']} descriptors.", file=t)
        print(f"#Author: {metdict['author_name']}", file=t)
        print(f"#Index: {TRAININGDATA_NAME}.index.csv", file=t)
    return(trainingfile)    

def indexheader(indexfile, metdict):
    ''' creates a header file which contains metrics and description for index file
    '''
    modlog.info("Building index header containing metrics and description")
    with open(indexfile, 'w') as t2:
        print(f"#Generated on {datetime.utcnow()} associated with challenge problem {INDEX_NAME}", file=t2)
        print(f"#Index of {metdict['exp#']} experiments, covering {metdict['featcount']} descriptors.", file=t2)
        print(f"#Author: {metdict['author_name']}", file=t2)
    return(indexfile)

def stateheader(statefile, stdict):
    '''appends calculated metrics to the header of the state set
    '''
    modlog.info('Building initial stateset file containing header with metrics')
    with open(statefile, 'w') as t:
        print(f"#State set generated on {datetime.utcnow()} associated with challenge problem {INDEX_NAME}", file=t)
        print(f"#{stdict['exp#']} possible experiments, covering 67 descriptors.", file=t)
        #The features for the state set are not yet automatically generated, this remains constant until coded.
#        print(f"#{stdict['exp#']} possible experiments, covering {stdict['featcount']} descriptors.", file=t)
        print(f"#Author: {stdict['author_name']}", file=t)
        print(f"#Index: {STATESET_NAME}.index.csv", file=t)
    return(statefile)

def linkheader(linkfile, stdict):
    ''' appends calculated metrics to the header of the link file
    '''
    modlog.info('building initial link file containing header with metrics')
    with open(linkfile, 'w') as t:
        print(f"#Link file generated on {datetime.utcnow()} associated with challenge problem {INDEX_NAME}", file=t)
        print(f"#{stdict['exp#']} possible experiments, covering 67 descriptors.", file=t)
        #The features for the state set are not yet automatically generated, this remains constant until coded.
#        print(f"#{stdict['exp#']} possible experiments, covering {stdict['featcount']} descriptors.", file=t)
        print(f"#Author: {stdict['author_name']}", file=t)
        print(f"#Index: {STATESET_NAME}.index.csv", file=t)
    return(linkfile)

def stateindexheader(statesetindexfile, stdict):
    ''' creates and appends header for the state set index file 
    '''
    modlog.info("Building state set index header containing metrics and description")
    with open(statesetindexfile, 'w') as t2:
        print(f"#Index of {stdict['exp#']} possible experiments associated with challenge problem {INDEX_NAME}", file=t2)
        print(f"#Author: {stdict['author_name']}", file=t2)
        print(f"#Generated on {datetime.utcnow()} for the perovskite dataset", file=t2)
    return(statesetindexfile)

def statemetrics(statedf, metdict):
    ''' generates metrics for stateset header creation
    '''
    modlog.info('Generating state set metrics for version repo headers')
    stdict = {}
    stdict['exp#'] = len(statedf)
    stdict['author_name'] = metdict['author_name']
    return(stdict)

def exportstateset(statespace, link):
    ''' converts the statespace and link to the versioned data repo format
    '''
    df = pd.read_csv('statesets/%s'%statespace, low_memory=False)
    df.rename(columns={ df.columns[0]: 'name'}, inplace=True)
    linkdf = pd.read_csv('statesets/%s' %link, low_memory=False)
    linkdf.rename(columns={ linkdf.columns[0]: 'name'}, inplace=True)
    df2 = pd.DataFrame()
    df2['name'] = df['name']
    df2['dataset'] = INDEX_NAME
    indexdf = df2

    df.drop(['name'], axis=1,inplace=True)
    linkdf.drop(['name'], axis=1,inplace=True)

    maindf = pd.concat([indexdf,df],axis=1)
    linkdf = pd.concat([indexdf,linkdf],axis=1)
    indexdf = indexdf.set_index(['dataset'])
    maindf = maindf.set_index(['dataset'])
    linkdf = linkdf.set_index(['dataset'])
    return(indexdf, maindf, linkdf)

def metricbuild(traindf):
    ''' skeleton metrics gathered from training data for repo commit
    '''
    command_dict = pd.read_csv('type_command.csv')
    feature_count = len(command_dict)
    author_name = input("Enter your name for versioned repository records: ") 
    modlog.info('Generating training data metrics for version repo headers')
    metdict = {}
    metdict['exp#'] = len(traindf)
    metdict['featcount'] = feature_count
    metdict['author_name'] = author_name
    return(metdict)

def exporttraining(df):
    ''' generates the appropriate verdata repo structure

    takes the training data generated in the report code and converts
    the structure to the expected for the versioned data repo.  Also 
    generates a corresponding index file.  Adds the relevant 
    descriptions to the header of each CSV -- as expected by the version
    data repo.
    '''
    modlog.info('Generating version repo ready data and index dataframes')
    df2 = df.copy()
    df2['name'] = df2.index
    df2['dataset'] = INDEX_NAME
    indexdf = df2[['name', 'dataset']]
    maindf = pd.concat([indexdf,df],axis=1)
    indexdf = indexdf.set_index(['dataset'])
    maindf = maindf.set_index(['dataset'])
    return(indexdf, maindf)

def writetrain(indexdf, traindf, metdict):
    indexfile = (TRAININGDATA_NAME+".index.csv")
    trainfile = (TRAININGDATA_NAME+".csv")
    trainfile = trainheader(trainfile, metdict)
    indexfile = indexheader(indexfile, metdict)
    with open(trainfile, 'a') as f:
        traindf.to_csv(f)
    with open(indexfile, 'a') as f2:
        indexdf.to_csv(f2)

def writestate(stateindexdf, statedf, stdict, linkdf):
    state_index_file = (STATESET_NAME + ".index.csv")
    state_file = (STATESET_NAME + ".csv")
    link_file = (LINK_NAME + ".csv")
    state_index_file = stateindexheader(state_index_file, stdict)
    state_file = stateheader(state_file, stdict)
    link_file = linkheader(link_file, stdict)
    with open(state_file, 'a') as f2:
        statedf.to_csv(f2)
    with open(state_index_file, 'a') as f:
        stateindexdf.to_csv(f)
    with open(link_file, 'a') as f3:
        linkdf.to_csv(f3)

def prepareexport(final_report_df, cli_statespace, link, crank_num, dataset_name):#, stateinchi):
    ''' generate version repo ready csv files

    calls on metrics generator to provide basic information 
    for header construction.  Calls out for txt file creation with 
    relevant headers and appends version repo ready csvs to the final
    files ready for upload to the version repo
    '''
    global TRAININGDATA_NAME
    global INDEX_NAME
    global STATESET_NAME
    global LINK_NAME

    if crank_num is None:
        crank_num = '0'

    INDEX_NAME = crank_num
    TRAININGDATA_NAME = crank_num + f".{dataset_name}"
    STATESET_NAME = crank_num + ".stateset"
    LINK_NAME = crank_num + ".link"

    ## Build the training.csv and associated index with the correct headers
    indexdf, traindf = exporttraining(final_report_df)
    metdict = metricbuild(traindf) #retrieve dictionary of training data metrics
    if crank_num:
        print(f'Exporting data to {crank_num}.{dataset_name} for version data upload')
        modlog.info(f'Exporting data to {crank_num}.{dataset_name} for version data upload')
        writetrain(indexdf, traindf, metdict)
    
    if cli_statespace is not None:
        ## Build statespace, statespace index and link files then export and write
        modlog.info('Generating version data repo ready state space')
        modlog.info('Generating version data repo ready link')
        stateindexdf, statedf, linkdf = exportstateset(cli_statespace, link)
        stdict = statemetrics(statedf, metdict)
        writestate(stateindexdf, statedf, stdict, linkdf)


    return traindf

