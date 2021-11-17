import pandas as pd
import re
from glob import glob
import os
import subprocess
import random
from tqdm import tqdm
tqdm.pandas()

pdflist = glob("../DATA/FINAL_SAMPLE/**/*.pdf")
pdict = {i.split("/")[-1]:i for i in pdflist}

orders = pd.read_csv("../DATA/FINAL_SAMPLE/combinedOrderData.csv", sep="\t")
tlist = sorted([f for f in glob("../DATA/FINAL_SAMPLE/*.csv") if "Transfer" in f])

transfer = pd.concat((pd.read_csv(t) for t in tlist), ignore_index=True)
transfer["cnr_number"] = transfer["CaseUniqueValue"].str.extract("([A-Z]{4}\d{12})")

# transfer.drop_duplicates(subset=["cnr_number"], inplace=True, keep="first")

clist = transfer["cnr_number"].unique().tolist()
csam = random.choices(clist, k=100)

tsam = transfer.loc[transfer['cnr_number'].isin(csam)]
tmerge = pd.merge(left=tsam, right=orders, on="cnr_number", how="left")
tmerge.dropna(subset=["PDFFileName"], inplace=True)

if not os.path.isdir("../DATA/TRANSFER_SAMPLE"):
    os.mkdir("../DATA/TRANSFER_SAMPLE")

tmerge.to_csv("../DATA/TRANSFER_SAMPLE/transferSample.csv", sep="\t")

def getPDF(x):
    fname = pdict[x]
    dest = f"../DATA/TRANSFER_SAMPLE/{x}"
    subprocess.call(["cp", fname, dest])

_ = tmerge["PDFFileName"].progress_apply(getPDF)
