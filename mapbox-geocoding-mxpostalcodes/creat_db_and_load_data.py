import sqlite3
import pandas as pd
import random as random
import os.path

db_name = 'postalCodes.sqlite3'

if not os.path.exists(db_name):
  df = pd.read_csv('./assets/toQueryPostalCodes.csv', delimiter=',', dtype='str')
  df = df.fillna('')
  df['mb_query'] = (df['codigoPostal'] + ' ' + df['ciudad'] + ' ' + df['estadoNombreCorto']).str.replace('  ',' ')
  df['http_status_code'] = ''
  df['http_reason'] = ''
  df['http_text'] = ''
  df['response_features_0_place_type'] = ''
  df['response_features_0_relevance'] = ''
  df['response_features_0_text'] = ''
  df['response_features_0_place_name'] = ''
  df['response_features_0_center_longitude'] = ''
  df['response_features_0_center_latitude'] = ''
  df['response_features_0_context_0_id'] = ''
  df['response_features_0_context_0_text'] = ''
  df['response_features_0_context_0_short_code'] = ''
  df['response_features_0_context_1_id'] = ''
  df['response_features_0_context_1_text'] = ''
  df['response_features_0_context_1_short_code'] = ''
  df['response_features_0_context_2_id'] = ''
  df['response_features_0_context_2_text'] = ''
  df['response_features_0_context_2_short_code'] = ''
  df['response_features_0_context_3_id'] = ''
  df['response_features_0_context_3_text'] = ''
  df['response_features_0_context_3_short_code'] = ''
  df['response_features_0_context_4_id'] = ''
  df['response_features_0_context_4_text'] = ''
  df['response_features_0_context_4_short_code'] = ''
  
  conn = sqlite3.connect('postalCodes.sqlite3')
  df.to_sql('postal', conn, if_exists='replace', index=True, index_label="id")
  conn.close()