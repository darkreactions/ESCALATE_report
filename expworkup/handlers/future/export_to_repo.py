import pandas as pd

index_name='0015'
trainingdata_name = index_name + "_trainingdata"

def exportindex():
    df = pd.read_csv('Final.csv', low_memory=False)
    df['dataset'] = index_name
    df['name'] = df['RunID_vial']
    outdf = pd.concat([df['dataset'],df['name']], axis=1)
    outdf = outdf.set_index(['dataset'])

    df.drop(['RunID_vial'],axis=1)
    maindf = pd.concat([df['dataset'],df['name'],df],axis=1)
    maindf = maindf.set_index(['dataset'])

    outdf.to_csv(index_name+".csv")
    maindf.to_csv(trainingdata_name+".csv")

#    print(outdf)
exportindex()
