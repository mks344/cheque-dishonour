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

orderData = pd.read_csv("../DATA/FINAL_SAMPLE/combinedOrderData.csv", sep="\t")
print("Found pre-processed orders")
# ppickle = glob("../DATA/TEXT_PICKLE/t*.pickle")
# pdfs = [i for g in ppickle for i in pickle.load(open(g,"rb"))]
pdfs = pickle.load(file=open("../DATA/pdfpickle.pickle","rb"))

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

    # # helper function
    # def examine(self):
    #     print(f"Case Type: {self.caseType}")
    #     print(f"Disposal: {self.dispNature}")
    #     print(f"NI Verify: {self.niVerify}")
    #     print(f"Absconding: {self.absconding}")
    #     print(f"Amount: {self.amount}")
    #     print(f"Award: {self.award}")
    #     print(f"Outcome: {self.outcome}")
    #     print(f"Judge Change: {self.judgeChange}")
    #     print(f"Jurisdiction: {self.jurisdiction}")
    #     print(f"Mediation: {self.mediation}")
    #     print(f"Plausible defence: {self.plausibleDefence}")
    #     print(f"Sole purpose: {self.multipleCheques}")
    #     print(f"Summons: {self.summons}")
        

# run regex to extract attributes
def process(case):
    l = (re.search("negotiable\W+instrument[s]*\W+act|\W+n\W*i\W+act", order.text, flags=re.I|re.DOTALL) for order in case.interimOrders+case.finalOrders)
    l = [i for i in l if i]
    m = re.search("negotiable\W+instrument[s]*\W+act|\W+n\W*i\W+act", case.actName, flags=re.I|re.DOTALL)
    if len(l)>0 and m:
        case.niVerify = "fromBoth"
    elif len(l)>0:
        case.niVerify = "fromOrder"
    elif m:
        case.niVerify = "fromActName"
    
    absconding = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:[sS]ection 72|accused.{,60}(?:out of station|not available|unavailable|not (?:come )*present)|absconding|proclamation|ex-parte|not appear|avoid.{,50}service)|presence of (?:the )*accused|bailable warrant|nbw|bw|process issued|none has appeared on behalf of the accused|none (?:on behalf|for) (?:the )*accused|summoned.{,50}(?:warrant|bw)|(?:warrant|bw).{,50}not received back|production warrant|section 446 cr\W*p\W*c\W*|presence of the accused cannot be secured).{,100}?\.", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.absconding = [a for a in absconding if len(a.lines) > 0]
    
    amount = (Features(lines=re.findall("(.{,100}(?:cheque.{,150}?(?:rs|rupees|₹)\W*\d+\W*).{,100}?\.) ", order.text.replace(",",""), flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.amount = [a for a in amount if len(a.lines) > 0]

    award = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:mutually settled|full and final settlement|settlement|settle.{,30}dispute|compounded|compounding|settled amount|pa(?:y|id) direct fine|pay fine|required to pay|compensation|section 357|imprisonment| si | ri |compromised|outstanding|criminal compoundable|lok adalat|conciliation|resolution).{,100}?\.) ", order.text.replace(",",""), flags=re.DOTALL), path=order.path, type=order.type) for order in case.finalOrders+case.interimOrders if order.lang=='en')
    case.award = [a for a in award if len(a.lines) > 0]

    # judgeChange = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:[Cc]ase [Tt]ransfer [Dd]etails).{,100}?\.) ", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders)
    # case.judgeChange = [a for a in judgeChange if len(a.lines) > 0]
        
    jurisdiction = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:[sS]ection 7|beyond territorial jurisdiction|(?:beyond|want of|question of|proper|lacking) jurisdiction|before concerned court|jurisdictional error|under jurisdiction of|file be sent to the court of|6472-74/dhc/gaz/g-1/2018|case.{,5} be transferred|vijay dhanuka|najima mamtaj|k\W*s joseph.{,5}phil{,2}ips carbon|section 142\W2\Wa|sub\W*section 2.{,20}section 142).{,100}?\.) ", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.jurisdiction = [a for a in jurisdiction if len(a.lines) > 0]
        
    mediation = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:mediation|mediator|alternate dispute resolution|\Wa\W*d\W*r\W|compromise|lok adalat|mutually settled|settled out of court|(?:mutual[ly]*|amicabl[ey]) settle|matter has been settled).{,100}?\.) ", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.mediation = [a for a in mediation if len(a.lines) > 0]

    plausibleDefence = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:accused led defen[cs]e|in (?:his|her) defence|moonshine|plausible|evidence in defence|claimed trial|defence evidence|pleaded not guilty|prove[sd] probable defence|claimed to be tried).{,100}?\.) ", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.plausibleDefence = [a for a in plausibleDefence if len(a.lines) > 0]

    multipleCheques = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:[Ss]ection 219|[sS]ection 220|same (?:person|parties)|same transaction|all the cheques|cheques|another cheque).{,100}?\.) ", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.multipleCheques = [a for a in multipleCheques if len(a.lines) > 0]

    summons = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:[Ss]ection 143|summarily|[Ss]ection 262|[Ss]ection 265|not exceeding|day.to.day|reasoned order|speaking order|\W[dcp]w\W|witness).{,100}?\.) ", order.text, flags=re.DOTALL), path=order.path, type=order.type) for order in case.interimOrders+case.finalOrders if order.lang=='en')
    case.summons = [a for a in summons if len(a.lines) > 0]

    outcome = (Features(lines=re.findall("\. ((?:[A-Z][a-z0-9]|\([a-z]+\)| \d+\. ).{,100}?(?:not guilty|acquitted|convicted|guilty|sentenced).{,100}?\.) ", order.text[-1000:], flags=re.DOTALL), path=order.path, type=order.type) for order in case.finalOrders+case.interimOrders if order.lang=='en')
    case.outcome = [a for a in outcome if len(a.lines) > 0]
    
    return case

# iterate through case data and orders
def caseCrunch(j):
    rmatch = orderData.loc[orderData['cnr_number']==j['cnr_number']].to_dict(orient="records")
    omatch = [o for o in pdfs if o.cnr==j['cnr_number']]
    ocombine = []
    for o in omatch:
        r = [i for i in rmatch if i['PDFFileName']==o.path.replace(".txt",".pdf")][0]
        x = OrderDetails(path=o.path.replace(".txt",".pdf"), text=o.text, lang=o.lang, cnr=o.cnr, number=r['OrderNumber'], date=r['OrderDate'], details=r['OrderDetails'], type=r['OrderType'])
        ocombine.append(x)
    case = Case(actName = str(j['UnderActs']),
                actSec = j['UnderSections'],
                caseNo = j['CombinedCaseNumber'],
                caseType = j['CaseType'],
                cnr = j['cnr_number'],
                courtName = j['CourtName'],
                dateDecision = j['DecisionDate'],
                dateFiled = j['DateFiled'],
                dateFirstHearing = j['FirstHearingDate'],
                dateReg = j['RegistrationDate'],
                dispNature = j['NatureOfDisposal'],
                distName = j['CourtDistrict'],
                judgeNJDG = j['Njdg_Judge_Name'],
                pet = j['Petitioner'],
                pAdv = j['PetitionerAdvocate'],
                resp = j['Respondent'],
                rAdv = j['RespondentAdvocate'],
                stage = j['CurrentStage'],
                status = j['CurrentStatus'],
                stateName = j['CourtState'],
                transfer = [Transfer(date=i[0], fromJudge=i[1], toJudge=i[2]) for i in j["transfer"] if i],
                uid = j['Id'],
                year = j['Year'],
                interimOrders = [o for o in ocombine if o.type=='Interim'],
                ipath = [o.path for o in ocombine if o.type=='Interim'],
                finalOrders = [o for o in ocombine if o.type=='Final'],
                fpath = [o.path for o in ocombine if o.type=='Final'])
    return process(case)

def ntDict(nt):
    nd = nt.copy()
    nd["interimOrders"] = [dict(n._asdict()) for n in nd["interimOrders"]]
    nd["finalOrders"] = [dict(n._asdict()) for n in nd["finalOrders"]]
    nd["absconding"] = [dict(n._asdict()) for n in nd["absconding"]]
    nd["amount"] = [dict(n._asdict()) for n in nd["amount"]]
    nd["award"] = [dict(n._asdict()) for n in nd["award"]]
    nd["outcome"] = [dict(n._asdict()) for n in nd["outcome"]]
    # nd["judgeChange"] = [dict(n._asdict()) for n in nd["judgeChange"]]
    nd["jurisdiction"] = [dict(n._asdict()) for n in nd["jurisdiction"]]
    nd["mediation"] = [dict(n._asdict()) for n in nd["mediation"]]
    nd["plausibleDefence"] = [dict(n._asdict()) for n in nd["plausibleDefence"]]
    nd["multipleCheques"] = [dict(n._asdict()) for n in nd["multipleCheques"]]
    nd["summons"] = [dict(n._asdict()) for n in nd["summons"]]
    # ft = ["absconding", "amount", "award", "outcome", "judgeChange", "jurisdiction", "mediation", "plausibleDefence", "multipleCheques", "summons"]
    ft = ["absconding", "amount", "award", "outcome", "jurisdiction", "mediation", "plausibleDefence", "multipleCheques", "summons"]
    for i in nd["interimOrders"]:
        for f in ft:
            i[f] = [n for n in nd[f] if n["path"]==i["path"]]
    for i in nd["finalOrders"]:
        for f in ft:
            i[f] = [n for n in nd[f] if n["path"]==i["path"]]
    for f in ft:
        del nd[f]
        
    return nd


# def main():
    # run everything
    # file list
# clist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "CaseInfo" in f])
#     # hlist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "History" in f])
# tlist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "Transfer" in f])
# olist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "Order" in f])

    # if not os.path.isfile("../DATA/jdata.pickle"):
        
    #     print("Read case data")
    #     caseData = pd.concat([pd.read_csv(caseCSV,
    #                                       usecols = ['Id', 'CombinedCaseNumber', 'CaseType', 'Year',
    #                                              'CourtName', 'DateFiled', 'Petitioner', 'PetitionerAdvocate', 'Respondent',
    #                                              'RespondentAdvocate', 'CurrentStage', 'CurrentStatus',
    #                                              'cnr_number', 'RegistrationDate', 'DecisionDate', 'NatureOfDisposal',
    #                                              'UnderActs', 'UnderSections', 'CourtState', 'CourtDistrict',
    #                                              'FirstHearingDate', 'Njdg_Judge_Name'],
    #                                       parse_dates=False) for caseCSV in clist], ignore_index=True)

    #     appellate = caseData.loc[(caseData["CourtName"].str.contains("district|session|appellate",flags=re.I)) | (caseData["Njdg_Judge_Name"].str.contains("district|session|appellate",flags=re.I))].index
    #     caseData.drop(appellate, inplace=True)
    #     jdata = caseData.to_dict(orient="records")
    #     pickle.dump(file=open("../DATA/jdata.pickle","wb"), obj=jdata)
    # else:
    #     jdata = pickle.load(file=open("../DATA/jdata.pickle","rb"))
        
print("Read order data")
    # if not os.path.isfile("../DATA/FINAL_SAMPLE/combinedOrderData.csv"):
    #     orderData = pd.concat([pd.read_csv(orderCSV,
    #                                      usecols=["OrderNumber", "OrderDate",
    #                                               "OrderDetails", "OrderType", "PDFFileName"],
    #                                      parse_dates=False) for orderCSV in olist], ignore_index=True)
    #     orderData['cnr_number'] = orderData['PDFFileName'].str.extract("Order_+([A-Z0-9]+)_")
    #     orderData.to_csv("../DATA/FINAL_SAMPLE/combinedOrderData.csv", sep="\t", index=None)
    # else:
    #     

jdata = [j for j in pickle.load(file=open("../DATA/caseTransJoin.pickle","rb")) if not re.search("district|session|appellate", str(j["CourtName"])+str(j["Njdg_Judge_Name"]), flags=re.I)]
jdata = [jdata[i:i+121520] for i in range(0,len(jdata)+1,121520)]
# if not os.path.isfile(f"../DATA/sample5.pickle"):
print(datetime.datetime.now())

# with ProcessPoolExecutor(max_workers=8) as exr:
#     crunch = list(exr.map(caseCrunch, tqdm(jdata), chunksize=8000))
jnum = int(sys.argv[1])

crunch = [caseCrunch(j) for j in tqdm(jdata[jnum])]

print("Done")

pickle.dump(file=open(f"../DATA/sample6_{jnum}.pickle", "wb"), obj=crunch)
print(datetime.datetime.now())

# ni = [c for c in cases if c["niVerify"]!="notNI"]
# rest = [c for c in cases if c["niVerify"]=="notNI" and (len(c["interimOrders"]) + len(c["finalOrders"])) > 0]

# eng = [n for n in ni if "en" in {i.lang for i in n["interimOrders"]+n["finalOrders"]}]
# json.dump(fp=open("../DATA/MANUAL_CHECK/engNI.json","w"), obj=eng)

# feat = [e for e in eng if len(e["absconding"] + e["award"] + e["amount"] + e["outcome"] + e["jurisdiction"] + e["mediation"] + e["plausibleDefence"] + e["multipleCheques"] + e["summons"]) > 0]

# noFeat = [ntDict(e) for e in eng if e not in feat]
# json.dump(fp=open("../DATA/MANUAL_CHECK/noFeatures.json","w"), obj=noFeat)

# engRest = [ntDict(n) for n in rest if "en" in {i.lang for i in n["interimOrders"]+n["finalOrders"]}]
# json.dump(fp=open("../DATA/MANUAL_CHECK/engNotNI.json","w"), obj=engRest)

# absconding = [ntDict(e) for e in feat if len(e["absconding"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/absconding.json","w"), obj=absconding)

# award = [ntDict(e) for e in feat if len(e["award"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/award.json","w"), obj=award)

# amount = [ntDict(e) for e in feat if len(e["amount"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/amount.json","w"), obj=amount)

# outcome = [ntDict(e) for e in feat if len(e["outcome"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/outcome.json","w"), obj=outcome)

# jurisdiction = [ntDict(e) for e in feat if len(e["jurisdiction"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/jurisdiction.json","w"), obj=jurisdiction)

# mediation = [ntDict(e) for e in feat if len(e["mediation"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/mediation.json","w"), obj=mediation)

# plausibleDefence = [ntDict(e) for e in feat if len(e["plausibleDefence"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/plausibleDefence.json","w"), obj=plausibleDefence)

# multipleCheques = [ntDict(e) for e in feat if len(e["multipleCheques"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/multipleCheques.json","w"), obj=multipleCheques)

# summons = [ntDict(e) for e in feat if len(e["summons"])>0]
# json.dump(fp=open("../DATA/MANUAL_CHECK/summons.json","w"), obj=summons)
