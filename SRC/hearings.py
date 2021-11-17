import pandas as pd
import re
from glob import glob
import pickle
from collections import Counter, defaultdict
from datetime import datetime, Timedelta

# ni data
df = pd.read_pickle("../DATA/caseConsolidated4.pickle")

# hearings data
hlist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "History" in f])
hcols = ['Id', 'CaseInformationId', 'HearingDate','PurposeOfHearing']
htab = pd.concat((pd.read_csv(h, usecols=hcols) for h in hlist), ignore_index=True)
htab = htab.loc[htab["CaseInformationId"].isin(df["uid"])]
htab["PurposeOfHearing"].fillna("NA", inplace=True)
htab["purp"] = htab["PurposeOfHearing"].str.lower().str.replace("\W+|\_","", regex=True).str.replace("sinedie|dormantcase","", regex=True)

# standardised hearings and stages
hear = pd.read_csv("../DATA/purpStd.csv", sep="\t")
hear.drop("Unnamed: 0", axis=1, inplace=True)
hear["purp"] = hear["purpose"].str.lower().str.replace("\W+|\_","", regex=True).str.replace("sinedie|dormantcase","", regex=True)

# join hearing stages with hearings table
hdf = pd.merge(left=htab, right=hear, on="purp", how="outer")
hdf["hstage"] = hdf["stage"].replace(".*EVIDENCE","EVIDENCE",regex=True)
hdf["hstage"].replace("-","NA",regex=False,inplace=True)
hdf["otherInfo"].replace("-","NA",regex=False,inplace=True)

# absconding
abscond = hdf.loc[hdf["otherInfo"].isin(["ABSCONDING","NON-APPEARANCE"]),"CaseInformationId"]
df["char_absconding"] = None
df.loc[df["uid"].isin(abscond),"char_absconding"] = 1
df["char_absconding"].fillna(df["absconding_count"].astype(int),inplace=True)

# summons
summons = hdf.loc[hdf["otherInfo"]=="SUMMONS-TRIAL","CaseInformationId"]
df["char_summons"] = None
df.loc[df["uid"].isin(summons),"char_summons"] = 1
df["char_summons"].fillna(df["summons_count"].astype(int),inplace=True)

# mediation
mediation = hdf.loc[hdf["otherInfo"]=="MEDIATION","CaseInformationId"]
df["char_mediation"] = None
df.loc[df["uid"].isin(mediation),"char_mediation"] = 1
df["char_mediation"].fillna(df["mediation_count"].astype(int),inplace=True)


# char summary
charCols = [c for c in df.columns if "char_" in c]
chars = df.groupby("stateName")[charCols].agg("sum").sort_index()
totals = df["stateName"].value_counts().sort_index()
chars = pd.concat([chars,totals], axis=1)
chars.rename(columns={"stateName":"total"}, inplace=True)
cpercCols = ["percent_"+c for c in charCols]
for c in charCols:
    chars[f"percent_{c}"] = (chars[c]/chars["total"]*100).round(1)

charDuration = []
for c in charCols:
    cdf = pd.crosstab(index=df["stateName"], columns=df[c], values=df["daysToDisposal"], aggfunc="mean")
    charDuration.append(cdf)
charDF = pd.concat(charDuration, axis=1)
ddur = df.groupby("stateName")["daysToDisposal"].mean()
charDF = pd.concat([charDF, ddur], axis=1)

# hearing stages
stages = hdf["hstage"].value_counts().index.tolist()
# stageHearings = [(h, "hnos_"+h) for h in stages]
# stageDuration = [(h, "duration_"+h) for h in stages]
# df[stageHearings] = None
# df[stageDuration] = None

# Number of hearings
hdf.sort_values(by=["CaseInformationId","HearingDate"],inplace=True,ascending=True)
hnos = hdf.groupby("CaseInformationId")["hstage"].agg(lambda x: Counter(x)).reset_index()
hnos[stages] = None
for s in stages:
    hnos[s] = hnos["hstage"].apply(lambda x: x[s], axis=1)
dmerge = pd.merge(left=df, right=hnos, left_on="uid", right_on="CaseInformationId", how="left")

hearingNos = dmerge.groupby("stateName")[stages].agg("median")
htot = dmerge.groupby("stateName")["hearingNos"].agg("median")
hnums = pd.concat([hearingNos, htot], axis=1).astype(int)
hnums.to_csv("../DATA/hearingStageNos.csv")

# hearing duration
def calcDuration(group):
    gstages = list(zip(group["hdates"],group["stages"]))
    startDate = None
    startStage = None
    startIx = None
    currentDate = None
    gdict = defaultdict(int)
    if len(gstages) ==1:
        return {gstages[0][1]:1}
    for ix,(hdate, stage) in enumerate(gstages):
        if startStage is None:
            startStage = stage
            startDate = hdate
            startIx = ix
        elif startStage is not None and startStage!=stage:
            if ix == startIx+1:
                duration = 1
                gdict[startStage]+=duration
                startStage = stage
                startDate = hdate
                currentDate = hdate
                startIx = ix
                if ix == len(gstages) - 1:
                    print(ix)
                    gdict[stage]+=1
            else:
                duration = (currentDate - startDate).days
                gdict[startStage]+=duration
                currentDate = hdate
                startDate = hdate
                startStage = stage
                startIx = ix
                if ix == len(gstages) - 1:
                    gdict[stage]+=1
        elif startStage is not None and ix == len(gstages) -1:
            duration = (hdate - startDate).days
            gdict[startStage]+=duration
        else:
            if hdate!=
            currentDate = hdate
    return gdict

hdf["hdiff"] = hdf.groupby("CaseInformationId")["HearingDate"].diff().dt.days
hdf["hdiff"] = hdf.groupby("CaseInformationId")["hdiff"].apply(lambda x : x.interpolate(method="linear"))
hdf["hdt"] = hdf["HearingDate"].fillna(method="ffill")

def fillDate(row):
     if row["HearingDate"]==row["hdt"]:
         return row["HearingDate"]
     try:
         d = timedelta(days=row["hdiff"])
         return row["hdt"] + d
     except ValueError:
         return

hdf["hdate"] = hdf.apply(fillDate, axis=1)

hd = hdf.groupby("CaseInformationId")["hdate"].agg(lambda x: x.tolist()).to_dict()
hs = hdf.groupby("CaseInformationId")["hstage"].agg(lambda x: x.tolist()).to_dict()
hdur = [{"id":k, "hdates":hd[k], "stages":hs[k]} for k in hd.keys()]

for group in hdur:
    group["duration"] = calcDuration(group)

hdays = [{"uid":h["id"],**h["duration"]} for h in hdur]

# x = [h for h in hdur if h["id"]=="fffb6faf-500b-41e9-9ed6-3314995c6b86"][0]

# columns
hdays = pd.DataFrame(hdays)
dmerge = pd.merge(left=dmerge, right=hdays, on="uid", how="left")
dstages = [f"duration_{s}" for s in stages]
hstages = [f"hearings_{s}" for s in stages]

dmerge.drop(["caseType","actName", "actSec","interimOrders", "ipath", "finalOrders", "fpath", "transfer", "niVerify", "absconding", "amount", "award", "outcome", "jurisdiction", "mediation", "plausibleDefence", "multipleCheques", "summons", "otext", "index", "absconding_count", "amount_count", "award_count", "jurisdiction_count", "mediation_count", "plausibleDefence_count", "multipleCheques_count", "summons_count", "outcome_count", "hstage"], axis=1).to_csv("../DATA/masterTable.csv", sep="\t")

# disposal
disp = pd.read_csv("../DATA/disposalTypesTagged.csv")
disp.loc[disp["conclusion"]=="-","conclusion"] = disp["dispCat"]
disp.rename(columns={"dispCat":"dispSubcat", "dispNature":"dispCat"}, inplace=True)
disp["dmatch"] = disp["dispCat"].str.lower().str.replace("\W","", regex=True)
disp.drop(["dispCat"], axis=1, inplace=True)
dispDedup = disp.drop_duplicates(subset=["contested", "dmatch"], keep="first")
dmerge["contested"] = dmerge["dispNature"].str.contains("^contested", regex=True, flags=re.I).astype(int)
dmerge["contested"].fillna("NA", inplace=True)
dmerge["dispCat"] = dmerge["dispNature"].str.replace("^contested|^uncontested","", flags=re.I|re.S, regex=True).str.replace("^\W+|\W+$","", regex=False)
dmerge["dmatch"] = dmerge["dispCat"].str.lower().str.replace("\W","", regex=True)
# join disp nature
dmerge = pd.merge(left=dmerge, right=dispDedup, on=["contested","dmatch"], how="left")

# missing characteristics
dmerge["char_jurisdiction"] = ((dmerge["jurisdiction"].str.len()>0) | (dmerge['tonce']=="Yes")).astype(int)
dmerge["char_multipleCheques"] = dmerge["multipleCheques_count"].astype(int)

# outcomes tables
dmerge.groupby(["contested","dispCat"]).count()

# pending cases
dmerge = dmerge.loc[dmerge["year"] > 2013].copy()
dmerge = dmerge.loc[dmerge["year"] < 2019].copy()
cutoff = datetime(year=2021,month=9, day=17)
pend = dmerge.loc[dmerge["status"]=="PENDING"].copy()
pend["duration"] = (cutoff - pend["dateFiled"]).dt.days/365
pend["dbins"] = pd.cut(pend["duration"], bins=[0,1,2,3,4,5,6,7,8,9,10])
x = pd.crosstab(pend["stateName"], pend["dbins"], margins=True, margins_name="Total")
x.to_csv("../DATA/pendingBins.csv", sep="\t")
y = pd.DataFrame(columns=x.columns)
for c in x.columns:
    y[c] = (x[c]/x["Total"]*100).round(1)
y["Total"] = x["Total"]
y.to_csv("../DATA/pendingBinsPercentage.csv", sep="\t")

# pend to disp
dmerge["dispYear"] = dmerge[""]
