import sqlite3
import pandas as pd
import random as random
import os.path

db_name = 'postalCodesLonLatMapboxQuery.sqlite3'




if not os.path.exists(db_name):
  df = pd.read_csv('./assets/postalCodesLonLatMapboxQuery.csv', delimiter='|', dtype='str')
  df["postalCode_relevance_group"] = df["postalCode_relevance_group"].astype(float)
  # df = df.fillna('')
  df['nasa_http_status_code'] = ''
  df['nasa_http_reason'] = ''
  df['nasa_http_text'] = ''
  conn = sqlite3.connect(db_name)
  df.to_sql('postalMapBoxQuery', conn, if_exists='replace', index=True, index_label="id")
  
  df2 = pd.DataFrame({'postalMapBoxQuery_id': pd.Series(dtype='str'), 'codigoPostal': pd.Series(dtype='str'), 'variable': pd.Series(dtype='str'), 'dimension': pd.Series(dtype='str'), 'value_type': pd.Series(dtype='str'), 'numeric_value': pd.Series(dtype='float'), 'string_value': pd.Series(dtype='str')})
  df2.to_sql('postalNasa', conn, if_exists='replace', index=True, index_label="id")

  conn.close()