import sqlalchemy as sq
import glob

clist = sorted(glob.glob("../DATA/FINAL_SAMPLE/*CaseInfo*.csv"))
hlist = sorted(glob.glob("../DATA/FINAL_SAMPLE/*History*.csv"))
tlist = sorted(glob.glob("../DATA/FINAL_SAMPLE/*Transfer*.csv"))

ccols = ['Id', 'CombinedCaseNumber', 'CaseType', 'Year', 'CourtName',
         'DateFiled', 'Petitioner', 'PetitionerAdvocate', 'Respondent', 'RespondentAdvocate', 'CurrentStatus',
         'District', 'BeforeHonarbleJudges', 'cnr_number', 'RegistrationDate', 'RegistrationNo', 'DecisionDate',
         'NatureOfDisposal', 'UnderActs', 'UnderSections', 'PoliceStation', 'CourtState', 'StageOfCase',
         'FirstHearingDate', 'Njdg_Judge_Name', 'FIRYear']

hcols = ['Id', 'CaseInformationId', 'BeforeHonourableJudges',
         'BusinessOnDate', 'HearingDate','PurposeOfHearing']

tcols = ['Id', 'CaseInformationId', 'TransferDate',
         'FromCourtJudge', 'ToCourtJudge']

