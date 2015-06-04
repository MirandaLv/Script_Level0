# Create a UUID for transaction table 
# Name: Miranda Lv
# Date: 2/16/2015

import uuid
from pandas import DataFrame as df
import pandas as pd
import shortuuid

inf = r"/home/snowhui/itpir_repo/minerva-colombia-geocoded-dataset/products/Level_0/new/transactions_new.tsv.csv"
outf = r"/home/snowhui/itpir_repo/minerva-colombia-geocoded-dataset/products/Level_0/new/transactions.tsv"

data = pd.read_csv(inf, sep='\t')

idlist = list(data.project_id)

transactionid = []

for i in idlist:
	#tranid = uuid.uuid4()
	tranid = shortuuid.uuid()
	transactionid.append(tranid)

data['transaction_id'] = transactionid

data.to_csv(outf, sep='\t', encoding='utf-8', index=False)


