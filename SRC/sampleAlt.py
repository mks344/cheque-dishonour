import glob
import logging
import databricks.koalas as pd
import pandas
import time
import os
from functools import partial
from tqdm import tqdm
tqdm.pandas()

def main():
    pd.set_option('compute.default_index_type', 'distributed')
    # load and join data
    flist = glob.glob("../DATA/cases/*.csv")
    print("Read DF")
    df = pd.concat([pd.read_csv(f, parse_dates=False) for f in flist], ignore_index=True)
    df = df.drop(['female_defendant', 'female_petitioner', 'female_adv_def', 'female_adv_pet', 'purpose_name'], axis=1)
    print("Read Acts")
    acts = pd.read_csv("../DATA/acts_sections.csv", parse_dates=False)
    print("Merge 1")
    df = df.merge(right=acts, on="ddl_case_id", how="left")
    court = pd.read_csv("../DATA/keys/cases_court_key.csv", parse_dates=False)
    df = df.merge(court, on=['court_no', 'dist_code', 'state_code', 'year'], how="left")
    print("Merge 2")
    df = df.loc[df["state_name"].isin(["Chandigarh", "Punjab", "Karnataka", "Haryana", "Delhi", "Andhra Pradesh", "Goa", "Maharashtra", "Gujarat", "Himachal Pradesh"])]
    
    del court, acts
        # custom summary function
    def writeSummary(f, suffix):
        print(f"Year {suffix}")
        y = f['year'].value_counts().reset_index()
        y.to_csv(f"../RESULTS/years_{suffix}.csv", num_files=1)
        print(f"State {suffix}")
        s = f['state_name'].value_counts().reset_index()
        s.to_csv(f"../RESULTS/states_{suffix}.csv", num_files=1)
        print(f"Act {suffix}")
        a = f['act'].value_counts().reset_index()
        a.to_csv(f"../RESULTS/acts_{suffix}.csv", num_files=1)
        print(f"Act Year {suffix}")
        ay = f.groupby(['act', 'year'])['year'].count().unstack(level=-1).reset_index()
        ay.to_csv(f"../RESULTS/yearXAct_{suffix}.csv", num_files=1)
        print(f"Act State {suffix}")
        sa = f.groupby(['act', 'state_name'])['year'].count().unstack(level=-1).reset_index()
        sa.to_csv(f"../RESULTS/actXstate_{suffix}.csv", num_files=1)
        print(f"State Year {suffix}")
        sy = f.groupby(['state_name', 'year'])['year'].count().unstack(level=-1).reset_index()
        sy.to_csv(f"../RESULTS/stateXyear_{suffix}.csv", num_files=1)
        print(f"Done {suffix}")
        return (y,s,a,ay,sa,sy)

    # dsum = df.apply(partial(writeSummary(suffix="overall")))
    dsum = writeSummary(df, suffix="overall")
    # random sample
    print("Sample 1")
    f = 500000/len(df)
    sample = df.sample(frac=f, random_state=42).reset_index()
    sample.to_pandas().to_csv("../DATA/sample.csv")
    # stratified sample
    # strats = df.groupby(["state_code", "year"])['ddl_case_id'].agg("count").unstack().melt(ignore_index=True).reset_index()
    # strats["weight"] = round(strats['value']*0.01, 0)
    # print("Sample 2")
    # states = df['state_name'].unique().tolist()
    # years = df['year'].unique().tolist()
    # # del df
    # combo = zip(states, years)
    # masks = (((df["state_name"]==state) & (df['year']==year)) for state, year in combo)
    # stSamList = pd.concat([df.loc[mask].sample(frac=0.01, random_state=42) for mask in masks], ignore_index=True).reset_index()
    # stSamList.to_csv("../DATA/stSample.csv", num_files=1)

    ssum = writeSummary(sample, suffix="random")
    # tsum = writeSummary(stSamList, suffix="strat")
    # compare = ["../RESULTS/compareYear.csv", "../RESULTS/compareState.csv", "../RESULTS/compareAct.csv", "../RESULTS/compareYearAct.csv", "../RESULTS/compareActState.csv", "../DATA/compareStateYear.csv"]

    # for d,s,t,p in zip(dsum,ssum,tsum, compare):
    #     ds = d.merge(right=s, left_index=True, right_index=True, suffixes=["","rand"], how="outer")
    #     dt = ds.merge(right=t, left_index=True, right_index=True, suffixes=["","strat"], how="outer")
    #     dt.to_csv(p)

if __name__ == "__main__":
    main()
