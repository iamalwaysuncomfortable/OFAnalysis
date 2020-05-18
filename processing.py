#!pip install --upgrade firebase-admin
import os
import pandas as pd
import firebase_admin
import requests
from io import StringIO
from firebase_admin import credentials
from firebase_admin import firestore

dynamic_link_aggregate_URL = 'https://docs.google.com/spreadsheet/ccc?key=1G3lXYx_p_wv63k4ZmADm_wR_SNffMWqZjpVMC-IMWps&output=csv'
dynamic_link_time_URL = 'https://docs.google.com/spreadsheet/ccc?key=1G3lXYx_p_wv63k4ZmADm_wR_SNffMWqZjpVMC-IMWps&gid=14439463&output=csv'
payment_keys = ['user','userNickName','date','time','payment_for', 'gross','net','epoch','isoDate']
payment_key_types = {'user':'string','userNickName':'string','date':'string','time':'string','payment_for':'string', 'gross':'float','net':'float','epoch':'int','isoDate':'string'}
tip_keys = ['user','userNickName','date', 'time','amount','post','epoch','isoDate']
tip_key_types = {'user':'string','userNickName':'string','date':'string','time':'string','amount':'float','post':'string','epoch':'int','isoDate':'string'}

dynamic_links = (dynamic_link_aggregate_URL, dynamic_link_time_URL)

def get_db_instance():
  cred = credentials.Certificate('secret.json')
  firebase_admin.initialize_app(cred)
  db = firestore.client()
  return db

def get_google_spreadsheet(spreadsheet_url):
  response = requests.get(spreadsheet_url)
  assert response.status_code == 200, 'Wrong status code'
  return response.content.decode()

def make_dynamic_link_data(dynamic_link_URLs):
  df_agg = pd.read_csv(StringIO(get_google_spreadsheet(dynamic_link_URLs[0])))
  df_time = pd.read_csv(StringIO(get_google_spreadsheet(dynamic_link_URLs[1])))
  return df_agg, df_time

def get_docs(user, latest_only=False):
    docs = db.collection(u'users').document(user).get().to_dict()
    return docs

def clean_docs(docs):
  payments = {key:[] for key in payment_keys}
  tips = {key:[] for key in tip_keys}
  for key in docs.keys():
    if key[:7] == "payment":
      for subkey in payment_keys:
        if payment_key_types[subkey] == 'string':
          payments[subkey].append(docs[key].get(subkey,''))
        elif payment_key_types[subkey] == 'float':
          payments[subkey].append(docs[key].get(subkey, 0.0))
        elif payment_key_types[subkey] == 'int':
          payments[subkey].append(docs[key].get(subkey, 0))
    elif key[:3] == "tip":
      for subkey in tip_keys:
        if tip_key_types[subkey] == 'string':
          tips[subkey].append(docs[key].get(subkey,''))
        elif tip_key_types[subkey] == 'float':
          tips[subkey].append(docs[key].get(subkey, 0.0))
        elif tip_key_types[subkey] == 'int':
          tips[subkey].append(docs[key].get(subkey, 0))
  return pd.DataFrame.from_dict(payments), pd.DataFrame.from_dict(tips)


try:
  df_agg, df_time = make_dynamic_link_data(dynamic_links)
  db = get_db_instance()
  docs = get_docs("borst_jessica@yahoo.com")
  payments, tips = clean_docs(docs)
except Exception as e:
  print(e)

sub_payment_count = payments[payments['payment_for']=='Subscription'].groupby(pd.Grouper(key='date')).count()['user']
sub_payments = payments[payments['payment_for']=='Subscription'].groupby(pd.Grouper(key='date')).sum()
sub_payments['sub_count'] = sub_payment_count
sub_payments = sub_payments.drop(['epoch'],axis=1)
idx = pd.period_range('2020-01-31', '2020-05-15').to_timestamp()
sub_payments.index = pd.DatetimeIndex(sub_payments.index)
sub_payments = sub_payments.reindex(index=idx, fill_value=0)
sub_payments.index = pd.DatetimeIndex(sub_payments.index)
df_time.index = pd.DatetimeIndex(df_time.Date)
df_time = df_time.sort_index(ascending=True)
df_time = df_time.fillna(0)
merged = sub_payments.join(df_time)
merged['fetlife'] = df_time[['https://gingersexkitten.com/pussypics',
       'https://gingersexkitten.com/boobiepic',
       'https://gingersexkitten.com/fetlife',
       'https://gingersexkitten.com/daddyfetlife',
       'https://gingersexkitten.page.link/fetlifebutt',
       'https://gingersexkitten.page.link/fetlifeonlyfans1']].sum(axis=1)

merged['facebook'] = df_time[['https://gingersexkitten.com/SHS', 'https://gingersexkitten.com/FBpromo']].sum(axis=1)
merged['instagram'] = df_time['https://gingersexkitten.com/IG']
merged['reddit'] = df_time['https://gingersexkitten.com/custom']