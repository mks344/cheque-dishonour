import pandas as pd
import re
from glob import glob

clist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "CaseInfo" in f])
hlist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "History" in f])
tlist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "Transfer" in f])

# orders
print("Ord read")
orderData = pd.read_csv("../DATA/FINAL_SAMPLE/combinedOrderData.csv", sep="\t")
orderCount = pd.crosstab(orderData["cnr_number"], orderData["OrderType"])
del orderData

print("Read case data")
caseData = pd.concat([pd.read_csv(caseCSV,
                                  usecols = ['Id', 'CombinedCaseNumber', 'CaseType', 'Year',
                                             'CourtName', 'DateFiled', 'Petitioner', 'Respondent',
                                             'CurrentStatus', 'cnr_number', 'RegistrationDate', 'DecisionDate',
                                             'UnderActs', 'UnderSections', 'CourtState', 'CourtDistrict',
                                             'Njdg_Judge_Name'],
                                  parse_dates=False) for caseCSV in clist], ignore_index=True)

# drop appeal and revision
appellate = caseData.loc[(caseData["CourtName"].str.contains("district|session|appellate",flags=re.I)) | (caseData["Njdg_Judge_Name"].str.contains("district|session|appellate",flags=re.I))].index
caseData.drop(appellate, inplace=True)
del appellate

# join case and order data
caseOrderJoin = pd.merge(left=caseData, right=orderCount, on="cnr_number", how="left")
del caseData
del orderCount

# transfers data
transferData = pd.concat([pd.read_csv(transCSV,
                                      usecols=['Id', 'CaseInformationId'],
                                      parse_dates=False) for transCSV in tlist], ignore_index=True)

transferData.rename(columns={"Id":"trId"."CaseInformationId":"Id"}, inplace=True)
trans = transferData["Id"].value_counts().reset_index()
del transferData

print("TCO join")
coTransJoin = pd.merge(left=caseOrderJoin, right=trans, on="Id", how="left")
del trans
del caseOrderJoin

# hearing data
hcols = ["CaseInformationId"]
hearData = pd.concat([pd.read_csv(transCSV,
                                      usecols=['Id', 'CaseInformationId'],
                                      parse_dates=False) for transCSV in hlist], ignore_index=True)

hearData = hearData["CaseInformationId"].value_counts().reset_index()

print("Join hearings to case data")
df = pd.merge(left=coTransJoin, right=hearData, left_on="Id", right_on="CaseInformationId", how="left")
del hearData
del coTransJoin

# summary tables
