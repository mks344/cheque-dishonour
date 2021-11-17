import pyxpdf
from pyxpdf.xpdf import PDFIOError, PDFSyntaxError
from glob import glob, iglob
from collections import namedtuple, defaultdict
import pickle
from tqdm import tqdm
import re
import os
import fasttext
import gc
import resource
import sys

# # resource increase
# soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
# resource.setrlimit(resource.RLIMIT_NOFILE, (524288, hard))

gc.enable()

if not os.path.isdir("../DATA/TEXT_PICKLE"):
    os.mkdir("../DATA/TEXT_PICKLE")

path_to_pretrained_model = '/home/scraper/lid.176.bin'
fmodel = fasttext.load_model(path_to_pretrained_model)

# define ad hoc data-structure for orders
Order = namedtuple("Order", ['path', 'text', 'lang', 'cnr'])

# preprocessing for regex
def preproc(text):
    temp = []
    y = re.sub(" S\.| u\W[sS]\W| r\Ww\W[Ss]", " section ", text)
    y = re.sub("&", "and", y)
    y = re.sub("[\n \x0c\r\t]+"," ",y)
    y = re.sub(r"([a-zA-Z])\.(\w)",r"\1 \2", y)
    y = re.sub(r"(\d+)\W*",r"\1 ",y)
    gc.collect()
    return y.replace("  "," ").lower()

# function to convert pdf to text
def convert(r):
    if not os.path.isfile(r):
        return None, None
    try:
        convert = pyxpdf.Document(r)
        t = convert.text()
        t = t.strip()
        text = preproc(t).strip()
        assert len(text) > 50
    except (Exception, PDFIOError):
        gc.collect()
        return None, None
    else:
        gc.collect()
        return r, text

def genOrder(paths, texts):
    cnrs = [re.search("Order_+([A-Z0-9]+)_",path).group(1) for path in paths]
    langs, scores = fmodel.predict(texts)
    czip = zip(paths,cnrs,texts,langs,scores)
    plist = [Order(path=path.split("/")[-1], cnr=cnr, text=text, lang="en") if (lang=="__label__en" and score > 0.6) else Order(path=path.split("/")[-1], cnr=cnr, text=text, lang="not-en") for path,cnr,text,lang,score in czip]
    return plist
    gc.collect()

# def genOrder(path, text):
#     cnr = re.search("Order_+([A-Z0-9]+)_",path).group(1)
#     lang, score = fmodel.predict(text)
#     if lang=="__label__en" and score > 0.6:
#          gc.collect()
#          return Order(path=path.split("/")[-1], cnr=cnr, text=text, lang="en")
#     else:
#          gc.collect()
#          return Order(path=path.split("/")[-1], cnr=cnr, text=text, lang="not-en")
 
fnum = sys.argv[1]
gnum = int(sys.argv[2])
flist = pickle.load(file=open(f"../DATA/TEXT_PICKLE/f{fnum}.pickle", "rb"))
flist = flist[gnum]
fchunks = [flist[i:i+10] for i in range(0,len(flist)+1, 10)]
for enum, chunk in enumerate(fchunks):
    if not os.path.isfile(f"../DATA/TEXT_PICKLE/t{fnum}_{enum}_{gnum}.pickle"):
        # pdfToText = [convert(f) for f in tqdm(chunk, desc=f"Convert chunk {enum}")]
        pdfToText = (convert(f) for f in tqdm(chunk, desc=f"Convert chunk {enum}", total=len(chunk)))
        # pdfToText = (convert(f) for f in chunk)
        try:
            paths, texts = zip(*[(p,t) for p,t in pdfToText if p])
        except ValueError:
            print(chunk, enum)
            gc.collect()
            continue
        # orders = [genOrder(path=p,text=t) for p,t in tqdm(pdfToText, total=len(chunk), desc=f"Chunk {enum}") if p]
        orders = genOrder(paths=paths, texts=list(texts))
        pickle.dump(file=open(f"../DATA/TEXT_PICKLE/t{fnum}_{enum}_{gnum}.pickle","wb"), obj=orders)
        del orders
        gc.collect()
