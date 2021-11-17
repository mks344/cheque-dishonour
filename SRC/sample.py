import dask
import dask.dataframe as dd
import glob
import logging
import pandas as pd
import webbrowser
import time
import os

# setup distributed computing client
from dask.distributed import Client
# client = Client(memory_limit='4GB')

def main():
    if not os.path.isdir("../DATA/RESULTS"):
        os.mkdir("../DATA/RESULTS")
        
    client = Client(n_workers=4, threads_per_worker=2, memory_limit='5GB', silence_logs=logging.ERROR)
    print("Cluster created")
    # # start dashboard
    # webbrowser.open(client.dashboard_link)

    # # column reference
    cols = ["ddl_case_id", "year", "state_code", "dist_code", "court_no", "cino", "judge_position", "female_defendant", "female_petitioner", "female_adv_def", "female_adv_pet", "type_name", "purpose_name", "disp_name", "date_of_filing", "date_of_decision", "date_first_list", "date_last_list", "date_next_list"]
    cdict = {c:'category' for c in cols}
    ac = "ddl_case_id,act,section,bailable_ipc,number_sections_ipc,criminal"
    acols = {a:'category' for a in ac.split(",")}
    cc = "year,state_code,state_name,district_name,dist_code,court_no,court_name"
    ccols = {a:'category' for a in cc.split(",")}

    # read tables
    df = dd.read_csv("../DATA/cases/cases*.csv", dtype=cdict) # case data
    acts = dd.read_csv("../DATA/acts_sections.csv", dtype=acols)

    # drop unwanted columns
    df = df.drop(['female_defendant', 'female_petitioner', 'female_adv_def', 'female_adv_pet', 'purpose_name'], axis=1)
    df = df.loc[df['year'].isin(['2014', '2015', '2016', '2017', '2018'])]
    # df = df.set_index("ddl_case_id")
    # acts = acts.set_index("ddl_case_id")
    df = df.merge(acts, on="ddl_case_id", how="left")

    # join courts and case tables
    court = dd.read_csv("../DATA/keys/cases_court_key.csv", dtype=ccols)
    court = court.repartition(npartitions=1).persist()
    df = dd.merge(df, court, on=['court_no', 'dist_code', 'state_code', 'year'], how="left")
    print("DF Categorize")
    df = df.categorize(columns=["year", "act", "state_name"])
    df = client.persist(df)

    # assign dtypes for efficiency
    # df = df.categorize(columns=['ddl_case_id', 'year', 'state_code', 'dist_code', 'court_no', 'type_name', 'disp_name'])
    # acts = acts.categorize(columns=['ddl_case_id', 'act', 'section', 'bailable_ipc', 'number_sections_ipc', 'criminal'])
    # court = court.categorize(columns=["year", "state_code", "state_name", "district_name", "dist_code", "court_no", "court_name"])
    
    # sanity check
    print("DF : ", df.shape)

    # draw 10% random sample
    print("Sample")
    sample = df.sample(frac=0.01, random_state=42)

    # merge acts and sample
    # sample = sample.merge(acts, on="ddl_case_id", how="left")
    # merge sample and court, district, state table
    # sample = sample.merge(court, on=['court_no', 'dist_code', 'state_code', 'year'], how="left")
    # sample = sample.categorize(columns=['ddl_case_id', 'year', 'state_name', 'act'])
    print("Sample : ", sample.shape)

    # summary stats whole df
    print("DF Summary")
    yearDF, stateDF, actDF, actYearDF, stateYearDF, actStateDF, yearSAMPLE, stateSAMPLE, actSAMPLE, actYearSAMPLE, stateYearSAMPLE, actStateSAMPLE = dask.compute(df['year'].value_counts(), df['state_name'].value_counts(),df['act'].value_counts(), df.pivot_table(index="act", columns="year", values="ddl_case_id", aggfunc="count"), df.pivot_table(index="state_name", columns="year", values="ddl_case_id", aggfunc="count"), df.pivot_table(index="act", columns="state_name", values="ddl_case_id", aggfunc="count"), sample['year'].value_counts(), sample['state_name'].value_counts(), sample['act'].value_counts(), sample.pivot_table(index="act", columns="year", values="ddl_case_id", aggfunc="count"), sample.pivot_table(index="state_name", columns="year", values="ddl_case_id", aggfunc="count"), sample.pivot_table(index="act", columns="state_name", values="ddl_case_id", aggfunc="count"))

    client.close()
    print("Writing")
    # write results to file
    sample.to_csv("../DATA/sample.csv", single_file=True)
    yearSAMPLE.to_csv("../DATA/RESULTS/yearWiseSample.csv", single_file=True)
    stateSAMPLE.to_csv("../DATA/RESULTS/stateWiseSample.csv", single_file=True)
    actSAMPLE.to_csv("../DATA/RESULTS/actWiseSample.csv", single_file=True)
    actYearSAMPLE.to_csv("../DATA/RESULTS/yearActSample.csv", single_file=True)
    stateYearSAMPLE.to_csv("../DATA/RESULTS/yearStateSample.csv", single_file=True)
    actStateSAMPLE.to_csv("../DATA/RESULTS/actStateSample.csv", single_file=True)
    
    yearDF.to_csv("../DATA/RESULTS/yearWiseDf.csv", single_file=True)
    stateDF.to_csv("../DATA/RESULTS/stateWiseDf.csv", single_file=True)
    actDF.to_csv("../DATA/RESULTS/actWiseDf.csv", single_file=True)
    actYearDF.to_csv("../DATA/RESULTS/yearActDf.csv", single_file=True)
    stateYearDF.to_csv("../DATA/RESULTS/yearStateDf.csv", single_file=True)
    actStateDF.to_csv("../DATA/RESULTS/actStateDf.csv", single_file=True)


if __name__ == "__main__":
    main()
