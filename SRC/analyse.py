import pandas as pd
import json
import re
from glob import glob, iglob
from collections import namedtuple, defaultdict
from dataclasses import dataclass, field
from typing import List
import pickle
from tqdm import tqdm
import os
from concurrent.futures import ProcessPoolExecutor
import datetime
import numpy as np
import random
import functools
import sys

# define ad hoc data-structure for orders
Order = namedtuple("Order", ['path', 'text', 'lang', 'cnr'])
OrderDetails = namedtuple("OrderDetails", ['path', 'text', 'lang', 'cnr', 'number', 'date', 'details', 'type'])
Features = namedtuple("Features", ["lines", "path", "type"])
Transfer = namedtuple("Transfer", ["date", "fromJudge", "toJudge"])

# main class for cases
@dataclass
class Case:
    """Class for tracking a case"""
    # main attributes
    actName: str
    actSec: str
    caseNo: str
    caseType: str
    cnr: str
    courtName: str
    dateDecision: str
    dateFiled: str
    dateFirstHearing: str
    dateReg: str
    dispNature: str
    distName: str
    judgeNJDG: str
    pet: str
    pAdv: str
    resp: str
    rAdv: str
    stage: str
    status: str
    stateName: str
    uid: str
    year: float

    interimOrders: List[Order]
    ipath: list
    finalOrders: List[Order]
    fpath: list

    transfer: List[Transfer]

    # other attributes
    niVerify: str = "notNI"
    absconding: list = field(default_factory=list)
    amount: list = field(default_factory=list)
    award: list = field(default_factory=list)
    outcome: list = field(default_factory=list)
    # judgeChange: list = field(default_factory=list)
    jurisdiction: list = field(default_factory=list)
    mediation: list = field(default_factory=list)
    plausibleDefence: list = field(default_factory=list)
    multipleCheques: list = field(default_factory=list)
    summons: list = field(default_factory=list)


    
print("Load data")
# load data
# files = glob("../DATA/sample6*.pickle")
# df = pd.DataFrame([i.__dict__ for f in files for i in pickle.load(file=open(f, "rb"))])
df = pd.read_pickle("../DATA/caseConsolidated3.pickle")

# regex patterns
absconding = re.compile("(.{100}(?:section\W*72|accused.{,60}?(?:out\W*of\W*station|not\W*available|unavailable|not.{,20}?present)|absconding|ex\W*parte|not\W*appear|avoid.{,50}service|presence{,15}?accused|bailable\W*warrant|nbw|bw|process\W*issued|none.{,30}?accused|summoned.{,50}?(?:warrant|bw)|(?:warrant|bw).{,50}?not\W*received\W*back|production\W*warrant|section\W*446\W*c\W*r\W*p\W*c\W*|presence\W*of\W*the\W*accused\W*cannot\W*be\W*secured).{,100}?\.)\s", flags=re.S)

jurisdiction = re.compile("(.{100}(?:section\W*7|beyond\W*territorial\W*jurisdiction|beyond\W*jurisdiction|want\W*of\W*jurisdiction|question\W*of\W*jurisdiction|proper\W*jurisdiction|lacking\W*jurisdiction|before\W*concerned\W*court|jurisdictional\W*error|under\W*jurisdiction\W*of|file\W*be\W*sent\W*to\W*the\W*court\W*of|6472\W74\Wdhc\Wgaz\Wg\W1\W2018|case.{,5}\W*be\W*transferred|vijay\W*dhanuka|najima\W*mamtaj|k\W*s\W*joseph.{,5}phil{,2}ips\W*carbon|section\W*142\W2\Wa|sub\W*section\W*2.{,20}section\W*142).{,100}?\.)\W", flags=re.S)

mediation = re.compile("(.{100}(?:mediation|mediator|alternate\W*dispute\W*resolution|\Wa\W*d\W*r\W|compromise|lok\W*adalat|mutually\W*settled|settled\W*out\W*of\W*court|mutual[ly]*\W*settle|amicabl[ey]\W*settle|matter\W*has\W*been\W*settled).{,100}?\.)\W", flags=re.S)

multipleCheques = re.compile("(.{100}(?:section\W*219|section\W*220|same\W*person|same\W*part[iy]|same\W*transaction|cheques|other\W*cheque).{,100}?\.)\W", flags=re.S)

summons = re.compile("(.{100}(?:section\W*143|summarily|summary|section\W*262|section\W*265|reasoned\W*order|speaking\W*order|\W[dcp]w\W|witness).{,100}?\.)\W*", flags=re.S)

outcome = re.compile("(.{100}(?:not\W*guilty|acquitted|convicted|guilty|sentenced).{,100}?\.)\W*", flags=re.S)

df = df.loc[df["isNI"]=="NI"].copy()
# apply regex
print("Absconding")
df["absconding"] = df["otext"].str.lower().str.findall(absconding)
print("Amount")
df["amount"] = df["otext"].str.lower().str.findall(amount)
print("Award")
df["award"] = df["otext"].str.lower().str.findall(award)
print("Jurisdiction")
df["jurisdiction"] = df["otext"].str.lower().str.findall(jurisdiction)
print("Mediation")
df["mediation"] = df["otext"].str.lower().str.findall(mediation)
print("Plausible defence")
df["plausibleDefence"] = df["otext"].str.lower().str.findall(plausible)
print("Multiple cheques")
df["multipleCheques"] = df["otext"].str.lower().str.findall(multipleCheques)
print("Summons")
df["summons"] = df["otext"].str.lower().str.findall(summons)
print("Outcome")
df["outcome"] = df["otext"].str.lower().str.findall(outcome)

scols = ["absconding", "amount", "award", "jurisdiction", "mediation", "plausibleDefence", "multipleCheques", "summons", "outcome"]
tcols = ["absconding", "jurisdiction", "mediation", "plausibleDefence", "multipleCheques", "summons"]
# scols = ["absconding", "amount", "award", "jurisdiction", "mediation", "plausibleDefence", "multipleCheques", "summons", "outcome"]

for s in scols:
    df[f"count_{s}"] = (df[s].str.len() > 0).astype(int)

ccols = [c for c in df.columns if "count_" in c]

# secondary case characteristics
sec = df.groupby(["stateName"])[ccols].sum()
sec.sort_index(inplace=True)
sec2 = sec.copy()
x = df["stateName"].value_counts()
x.sort_index(inplace=True)
y = df.groupby("stateName")["engAny"].agg(lambda x: (x>0).sum())
y.sort_index(inplace=True)
sec["Total"] = x
sec["withEngOrder"] = y
sec.to_csv("RESULTS/caseChars.csv")

for c in ccols:
    sec2[c] = (sec2[c]/sec2["withEngOrder"]*100).round(1)

sec2["percentEngOrder"] = (sec2["withEngOrder"]/sec2["Total"]*100).round(1)
sec2.to_csv("RESULTS/caseCharsPercent.csv")

# outcomes
# df["contested"] = None
# df["dispNature"].fillna("NA", inplace=True)
# df["dispStd"] = df["dispNature"].str.replace("[^a-zA-Z\- /0-9\(\)]+"," ").str.lower()
# df.loc[df["dispStd"].str.contains("^uncontested"),"contested"] = 0
# df.loc[df["dispStd"].str.contains("^contested"),"contested"] = 1
# df["dispSubType"] = df["dispStd"].str.split("uncontested\-|contested\-").str[1]
# df["dispSubType"].fillna("NA", inplace=True)
# df["dispSubStd"] = None
# df.loc[df["dispSubType"]=="NA","dispSubStd"] = "NA"
# # x = sorted(df.loc[df["dispSubStd"].isnull(), "dispSubType"].unique().tolist())
# df.loc[(df["dispSubType"].str.contains("convict|imprison")) & (df["dispSubStd"].isnull()),"dispSubStd"] = "convicted"
# df.loc[(df["dispSubType"].str.contains("acquit")) & (df["dispSubStd"].isnull()),"dispSubStd"] = "acquitted"
# df.loc[(df["dispSubType"].str.contains("withdraw")) & (df["dispSubStd"].isnull()),"dispSubStd"] = "withdrawn"
# df.loc[(df["dispSubType"].str.contains("diss*mis|not pressed|drop|stop")) & (df["dispSubStd"].isnull()),"dispSubStd"] = "dismissed"
# df.loc[(df["dispSubType"].str.contains("lok|l/a|adr|media|concilliation")) & (df["dispSubStd"].isnull()),"dispSubStd"] = "settled_mediation"
# df.loc[(df["dispSubType"].str.contains("compromise|paid|compo*und|out\W*side|settle|consent|satis")) & (df["dispSubStd"].isnull()),"dispSubStd"] = "settled/compounded"

# hearings join
hear = pd.read_csv("../DATA/purpStd.csv", sep="\t")
hear.drop("Unnamed: 0", axis=1, inplace=True)
