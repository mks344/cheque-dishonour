import pandas as pd
import glob

f = glob.glob("../DATA/FINAL_SAMPLE/*History*.csv")

print("Concat")
df = pd.concat((pd.read_csv(tab,usecols=["PurposeOfHearing"]) for tab in f), ignore_index=True)
print("Drop")
df.drop_duplicates(keep="first", inplace=True)
print("sort")
df.sort_values("PurposeOfHearing")
print("Reset")
df.reset_index(inplace=True)
print("write")
df.to_csv("../DATA/hearingPurpose.csv", sep="\t")
