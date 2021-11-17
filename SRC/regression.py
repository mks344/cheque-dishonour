import pandas as pd
import statsmodels.formula.api as sm

# disposed
rdf = dmerge.loc[dmerge["status"]=="DISPOSED"].copy()
rdf.rename(columns={s:f"hearings_{s}" for s in stages}, inplace=True)
rdf = rdf.loc[rdf["year"]>2013].copy()
rdf = dmerge.loc[dmerge["status"]=="DISPOSED"].copy()
rdf.rename(columns={s:f"hearings_{s}" for s in stages}, inplace=True)
rdf = rdf.loc[rdf["year"]<2019].copy()
rdf["year"] = rdf["year"].astype(int).astype(str)

cols = ['distName', 'judgeNJDG', 'stateName', 'year',
        'hearingNos', 'daysToFirstHearing', 'daysToDisposal',
        'char_absconding', 'char_summons', 'char_mediation',
        'hearings_EVIDENCE', 'hearings_PRE-TRIAL', 'hearings_NA', 'hearings_FRAMING OF CHARGES',
        'hearings_313 STATEMENT', 'hearings_JUDGMENT', 'hearings_POST-JUDGMENT',
        'duration_PRE-TRIAL', 'duration_EVIDENCE', 'duration_JUDGMENT',
        'duration_NA', 'duration_313 STATEMENT', 'duration_FRAMING OF CHARGES',
        'duration_POST-JUDGMENT', 'contested', 'dispCat', 'conclusion', 'dispSubcat',
        'char_jurisdiction', 'char_multipleCheques']

c = ['actName', 'actSec', 'caseNo', 'caseType', 'cnr', 'courtName',
       'dateDecision', 'dateFiled', 'dateFirstHearing', 'dateReg',
       'dispNature', 'distName', 'judgeNJDG', 'pet', 'pAdv', 'resp', 'rAdv',
       'stage', 'status', 'stateName', 'uid', 'year', 'engInterim', 'interimNos', 'finalNos',
       'engFinal', 'anyOrderNos', 'engAny', 'hearingNos',
       'isNI', 'daysToFirstHearing', 'daysToDisposal', 'absconding_count',
       'tonce', 'char_nonAppearance', 'char_summons',
       'char_mediation', 'hearings_EVIDENCE',
       'hearings_PRE-TRIAL', 'hearings_NA', 'hearings_FRAMING OF CHARGES',
       'hearings_313 STATEMENT', 'hearings_JUDGMENT', 'hearings_POST-JUDGMENT',
       'duration_PRE-TRIAL', 'duration_EVIDENCE', 'duration_JUDGMENT',
       'duration_NA', 'duration_313 STATEMENT', 'duration_FRAMING OF CHARGES',
       'duration_POST-JUDGMENT', 'contested', 'dispCat', 
       'conclusion', 'dispSubcat', 'char_jurisdiction', 'char_multipleCheques',
       'dsubcat']

stages = ['EVIDENCE', 'PRE-TRIAL', 'NA', 'FRAMING OF CHARGES', '313 STATEMENT', 'JUDGMENT', 'POST-JUDGMENT']
dstages = [f"duration_{s}" for s in stages]
hstages = [f"hearings_{s}" for s in stages]
charCols = [c for c in dmerge.columns if "char_" in c]

rdf.rename(columns={"char_absconding":"char_nonAppearance"}, inplace=True)
rdf.dropna(subset=["hearingNos"], inplace=True)

rdf.rename(columns = {"charnonAppearance":"charNonAppearance", "charsummons":"charSummons", "charmediation":"charMediation", "charjurisdiction":"charJurisdiction", "charmultipleCheques":"charMultipleCheques"}, inplace=True)

dstages = [c for c in rdf.columns if "duration" in c]

# chars vs hearings
fe_ols = sm.ols(formula="hearingNos ~ 1 + C(stateName) + C(year) + charNonAppearance + charSummons + charMediation + charJurisdiction + charMultipleCheques + contested", data=rdf).fit()
with open("../RESULTS/feOLS_hearingNos.txt","w") as res:
    res.write(fe_ols.summary().as_text())

fe_ols.summary()

fe_ols = sm.ols(formula="hearingNos ~ 1 + C(stateName) + charNonAppearance + charSummons + charMediation + charJurisdiction + charMultipleCheques + contested", data=rdf).fit()
with open("../RESULTS/feOLS_hearingNosNoYear.txt","w") as res:
    res.write(fe_ols.summary().as_text())

fe_ols = sm.ols(formula="hearingNos ~ 1 + charNonAppearance + charSummons + charMediation + charJurisdiction + charMultipleCheques + contested", data=rdf).fit()
with open("../RESULTS/feOLS_hearingNosCharsOnly.txt","w") as res:
    res.write(fe_ols.summary().as_text())

# chars vs duration
fe_ols = sm.ols(formula="daysToDisposal ~ C(stateName) + C(year) + charNonAppearance + charSummons + charMediation + charJurisdiction + charMultipleCheques + contested", data=rdf).fit()
with open("../RESULTS/feOLS_duration.txt","w") as res:
    res.write(fe_ols.summary().as_text())

fe_ols = sm.ols(formula="daysToDisposal ~ 1 + C(stateName) + charNonAppearance + charSummons + charMediation + charJurisdiction + charMultipleCheques + contested", data=rdf).fit()
with open("../RESULTS/feOLS_durationNoYear.txt","w") as res:
    res.write(fe_ols.summary().as_text())

fe_ols = sm.ols(formula="daysToDisposal ~ 1 + charNonAppearance + charSummons + charMediation + charJurisdiction + charMultipleCheques + contested", data=rdf).fit()
with open("../RESULTS/feOLS_durationCharsOnly.txt","w") as res:
    res.write(fe_ols.summary().as_text())


# no constant
