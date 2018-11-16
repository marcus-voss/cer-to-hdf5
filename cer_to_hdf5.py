import pandas as pd
import numpy as np
import os
from datetime import datetime

def read_cer(path):  
    df = pd.read_csv(path, delim_whitespace=True, header=None, names=["ID", "datacode", "power"])

    # maybe du to summer time change
    df = df.drop_duplicates(["ID", "datacode"])

    # pivot households as columns
    df = df.pivot(index="datacode", columns="ID", values="power")

    # translate their date format to actual time index
    df["hour"] = pd.to_timedelta(df.index.astype(str).str.slice(3,5).astype(float) / 48, unit='day')
    df["hour"] = df["hour"].dt.round('30min') 
    df["date"] = pd.Timestamp(2009, 1, 1) + pd.to_timedelta(df.index.astype(str).str.slice(0,3).astype(int)-1 , unit='d')
    df["datetime"] = df["date"] + df["hour"] 
    df = df.set_index("datetime")
    df = df.drop(["hour", "date"], axis=1)
    df = df[~df.index.duplicated(keep='first')]
    df = df.reindex(pd.DatetimeIndex(start=df.index[0], end=df.index[-1], freq="30min"))

    # only keep whole year of 2010
    df = df["2010"]

    # # fix smaller holes
    df = df.interpolate(limit=6)

    # fix larger holes
    for c in df:
        df[c] = df[c].fillna(value=df[c].shift(48)) 

    # drop all that still have nan
    df = df.dropna(axis=1, how="any")
    return df

def transpose_dayahead(col, H_steps=48):
    X = np.zeros((len(col), H_steps))
    
    X[:, 0] = col.values.ravel()
    for h in np.arange(1, H_steps):
        X[:,h] = np.roll(col.values.ravel(), -h)
    
    X = pd.DataFrame(X, index=col.index)
    return X[(X.index.hour == 0) & (X.index.minute == 0)]


if __name__ == "__main__":

    # read in the 6 files File1.txt...File6.txt
    dfs = []
    for f in [f for f in os.listdir(".") if f.startswith("File")]:
        df = read_cer(f)
        dfs.append(df)
    
    # write to single table with one column per household
    result = pd.concat(dfs, axis=1)
    result.to_hdf('CER.h5', key='data', mode='w')

    # add meta information from Excelsheet
    meta = pd.read_excel("SME and Residential allocations.xlsx", index_col=0).iloc[:,[0]]
    meta[meta.Code==1] = "Residential"
    meta[meta.Code==2] = "SME"
    meta[meta.Code==3] = "Other"
    meta = meta[meta.index.isin(result.columns)]
    meta.to_hdf('CER.h5', key='meta', mode='a')

    # write to one table per household with one line per day
    for c in result:
        df = transpose_dayahead(result[[c]]).to_hdf('CER_trans.h5', key="ID" + str(c), mode='a')

    # update meta index to match id keys
    meta.index = "ID" + meta.index.astype(str)
    meta.to_hdf('CER_trans.h5', key='meta', mode='a')