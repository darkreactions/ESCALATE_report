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

index_name='0020'
trainingdata_name = index_name + ".perovskitedata"
state_name = index_name + ".stateset"
link_name = index_name + ".link"


modlog = logging.getLogger('report.export_to_repo')

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
# The name "first_run" is not significant.

def trainheader(trainingfile, metdict):
    ''' generates metrics for training data header creation
    '''
    modlog.info('Building initial training text file containing header with metrics')
    with open(trainingfile, 'w') as t:
        print('#Training data generated on %s for the perovskite dataset' %(datetime.utcnow()), file=t)
        print('#%s experiments, covering 67 descriptors.'%metdict['exp#'], file=t)
        print('#Author: Ian Pendleton', file=t)
        print('#Index: %s.index.csv' %trainingdata_name, file=t)
    return(trainingfile)    

def indexheader(indexfile, metdict):
    ''' creates a header file which contains metrics and description for index file
    '''
    modlog.info("Building index header containing metrics and description")
    with open(indexfile, 'w') as t2:
        print('#Index of %s experiments associated with challenge problem %s' %(metdict['exp#'],index_name) , file=t2)
        print('#Author: Ian Pendleton', file=t2)
        print('#Generated on %s for the perovskite dataset' %(datetime.utcnow()), file=t2)
    return(indexfile)

def stateheader(statefile, stdict):
    '''appends calculated metrics to the header of the state set
    '''
    modlog.info('Building initial stateset file containing header with metrics')
    with open(statefile, 'w') as t:
        print('#State set generated on %s associated with challenge problem %s' %(datetime.utcnow(), index_name), file=t)
        print('#%s possible experiments, covering 67 descriptors.'%stdict['exp#'], file=t)
        print('#Author: Ian Pendleton', file=t)
        print('#Index: %s.index.csv' %state_name, file=t)
    return(statefile)

def linkheader(linkfile, stdict):
    ''' appends calculated metrics to the header of the link file
    '''
    modlog.info('building initial link file containing header with metrics')
    with open(linkfile, 'w') as t:
        print('#Link file generated on %s associated with challenge problem %s' %(datetime.utcnow(), index_name), file=t)
        print('#%s possible experiments, covering 67 descriptors.'%stdict['exp#'], file=t)
        print('#Author: Ian Pendleton', file=t)
        print('#Index: %s.index.csv' %state_name, file=t)
    return(linkfile)

def stateindexheader(statesetindexfile, stdict):
    ''' creates and appends header for the state set index file 
    '''
    modlog.info("Building state set index header containing metrics and description")
    with open(statesetindexfile, 'w') as t2:
        print('#Index of %s possible experiments associated with challenge problem %s' %(stdict['exp#'],index_name) , file=t2)
        print('#Author: Ian Pendleton', file=t2)
        print('#Generated on %s for the perovskite dataset' %(datetime.utcnow()), file=t2)
    return(statesetindexfile)

def statemetrics(statedf):
    ''' generates metrics for stateset header creation
    '''
    modlog.info('Generating state set metrics for version repo headers')
    stdict = {}
    stdict['exp#'] = len(statedf)
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
    df2['dataset'] = index_name
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
    modlog.info('Generating training data metrics for version repo headers')
    metdict = {}
    metdict['exp#'] = len(traindf)
    return(metdict)

def exporttraining(finalcsv):
    ''' generates the appropriate verdata repo structure

    takes the training data generated in the report code and converts
    the structure to the expected for the versioned data repo.  Also 
    generates a corresponding index file.  Adds the relevant 
    descriptions to the header of each CSV -- as expected by the version
    data repo.
    '''
    modlog.info('Generating version repo ready data and index dataframes')
    df2 = pd.DataFrame()
    df = pd.read_csv(finalcsv, low_memory=False)
    df2['name'] = df['RunID_vial']
    df2['dataset'] = index_name
    df.drop(['RunID_vial'],axis=1,inplace=True)
    indexdf = df2
    #pd.concat([df2['name'],df2['name']], axis=1)
    maindf = pd.concat([indexdf,df],axis=1)
    indexdf = indexdf.set_index(['dataset'])
    maindf = maindf.set_index(['dataset'])
    return(indexdf, maindf)

def writetrain(indexdf, traindf, metdict):
    indexfile = (trainingdata_name+".index.csv")
    trainfile = (trainingdata_name+".csv")
    trainfile = trainheader(trainfile, metdict)
    indexfile = indexheader(indexfile, metdict)
    with open(trainfile, 'a') as f:
        traindf.to_csv(f)
    with open(indexfile, 'a') as f2:
        indexdf.to_csv(f2)

def writestate(stateindexdf, statedf, stdict, linkdf):
    state_index_file = (state_name + ".index.csv")
    state_file = (state_name + ".csv")
    link_file = (link_name + ".csv")
    state_index_file = stateindexheader(state_index_file, stdict)
    state_file = stateheader(state_file, stdict)
    link_file = linkheader(link_file, stdict)
    with open(state_file, 'a') as f2:
        statedf.to_csv(f2)
    with open(state_index_file, 'a') as f:
        stateindexdf.to_csv(f)
    with open(link_file, 'a') as f3:
        linkdf.to_csv(f3)

def prepareexport(trainingname, statespace, link):#, stateinchi):
    ''' generate version repo ready csv files

    calls on metrics generator to provide basic information 
    for header construction.  Calls out for txt file creation with 
    relevant headers and appends version repo ready csvs to the final
    files ready for upload to the version repo
    '''
    ## Build statespace, statespace index and link files then export and write
    modlog.info('Generating version data repo ready state space')
    modlog.info('Generating version data repo ready link')
    stateindexdf, statedf, linkdf = exportstateset(statespace, link)
    stdict = statemetrics(statedf)
    writestate(stateindexdf, statedf, stdict, linkdf)
    
    ## Build the training.csv and associated index with the correct headers
    print('Exporting data to %s.csv for version data upload' %index_name)
    indexdf, traindf = exporttraining(trainingname)
    metdict = metricbuild(traindf) #retrieve dictionary of training data metrics
    writetrain(indexdf, traindf, metdict)


if __name__ == "__main__":
    prepareexport('test.csv', 'EtNH3Istateset.csv', 'EtNH3Istateset_link.csv')

