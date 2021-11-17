import pickle
import pandas as pd
import re
from collections import namedtuple, defaultdict

Order = namedtuple("Order", ['path', 'text', 'lang', 'cnr'])
OrderDetails = namedtuple("OrderDetails", ['path', 'text', 'lang', 'cnr', 'number', 'date', 'details', 'type'])
Features = namedtuple("Features", ["lines", "path", "type"])

