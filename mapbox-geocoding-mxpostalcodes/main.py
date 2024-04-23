import json
import requests
import pandas as pd
import csv
import time
import sqlite3

mapbox_access_token = 'pk.eyJ1IjoiZG5sYm5scyIsImEiOiJjbGxqdTl2NGIxcnl2M2twa2p6Y3p4bTQ2In0.gG1vNxrKrowI5BbPdywgkw'
# mapbox_access_token = 'pk.eyJ456'


# Codigo para consultar Mapbox
def fetch_geocode_data(query, access_token, country=None, limit=None):
  # Mapbox Geocoding API Endpoint
  url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"

  # Define the parameters
  params = {
      'access_token': access_token,
      'country': country,  # If specified, restricts search to a specific country
      'limit': limit,       # If specified, limits the number of results returned
      'types': 'postcode'
  }

  # Remove None values from parameters
  params = {k: v for k, v in params.items() if v is not None}

  headers = {
      'Accept': 'application/json'
  }

  try:
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
      return response
    elif response.status_code == 401:
      print (f"Unauthorized - check your access token: {response.status_code}")
      return response
    elif response.status_code == 403:
      print (f"Forbidden - access token may not have proper permissions: {response.status_code}")
      return response
    elif response.status_code == 404:
      print (f"Location not found: {response.status_code}")
      return response
    elif response.status_code == 429:
      print (f"Too many requests - rate limit exceeded: {response.status_code}")
      return response
    elif response.status_code > 429:
      print(f"Your request has failed with status code: {response.status_code}")
      return response
    else:
      response.raise_for_status()
      return response
  except requests.exceptions.ConnectionError as error:
    print (f"Request error: {error}")
    return


# Conectarse a la base de datos y cargar la tabla de codigos postales que todavia no han sido procesados en un dataframe
conn = sqlite3.connect('postalCodes.sqlite3')
query = "SELECT * FROM postal WHERE http_status_code = '' OR http_status_code != '200'"
# read data, load to dataframe and shuffle randomly
df = pd.read_sql(query, conn).sample(frac=1).reset_index(drop=True)
conn.close()


# Iterar sobre dataframe
for index, row in df.iterrows():
  try:

    response = fetch_geocode_data(row['mb_query'], mapbox_access_token, "MX", 1)
    time.sleep(0.100)

    df.at[index, 'http_status_code'] = getattr(response, 'status_code', '')
    df.at[index, 'http_reason'] = getattr(response, 'reason', '')
    df.at[index, 'http_text'] = getattr(response, 'text', '')

    if response.status_code == 200:
      res = json.loads(response.text)

      if "features" in res:
        if len(res["features"]) > 0:

          print(f"\033[92m{index+1}: RESPONSE 200: FEATURES INCLUDED. SAVING IN DATAFRAME")

          resFeatures = res["features"][0]
          df.at[index, 'response_features_0_place_type']           = resFeatures['place_type'][0] if ('place_type' in resFeatures) else ''
          df.at[index, 'response_features_0_relevance']            = resFeatures['relevance'] if ('relevance' in resFeatures) else ''
          df.at[index, 'response_features_0_text']                 = resFeatures['text'] if ('text' in resFeatures) else ''
          df.at[index, 'response_features_0_place_name']           = resFeatures['place_name'] if ('place_name' in resFeatures) else ''
          df.at[index, 'response_features_0_center_longitude']     = resFeatures['center'][0] if ('center' in resFeatures) else ''
          df.at[index, 'response_features_0_center_latitude']      = resFeatures['center'][1] if ('center' in resFeatures) else ''
          if("context" in resFeatures):
            resFeaturesContext = resFeatures['context']
            if len(resFeaturesContext) > 0:
              df.at[index, 'response_features_0_context_0_id']         = resFeaturesContext[0]['id'] if ('id' in resFeaturesContext[0]) else ''
              df.at[index, 'response_features_0_context_0_text']       = resFeaturesContext[0]['text'] if ('text' in resFeaturesContext[0]) else ''
              df.at[index, 'response_features_0_context_0_short_code'] = resFeaturesContext[0]['short_code'] if ('short_code' in resFeaturesContext[0]) else ''
            if len(resFeaturesContext) > 1:
              df.at[index, 'response_features_0_context_1_id']         = resFeaturesContext[1]['id'] if ('id' in resFeaturesContext[1]) else ''
              df.at[index, 'response_features_0_context_1_text']       = resFeaturesContext[1]['text'] if ('text' in resFeaturesContext[1]) else ''
              df.at[index, 'response_features_0_context_1_short_code'] = resFeaturesContext[1]['short_code'] if ('short_code' in resFeaturesContext[1]) else ''
            if len(resFeaturesContext) > 2:
              df.at[index, 'response_features_0_context_2_id']         = resFeaturesContext[2]['id'] if ('id' in resFeaturesContext[2]) else ''
              df.at[index, 'response_features_0_context_2_text']       = resFeaturesContext[2]['text'] if ('text' in resFeaturesContext[2]) else ''
              df.at[index, 'response_features_0_context_2_short_code'] = resFeaturesContext[2]['short_code'] if ('short_code' in resFeaturesContext[2]) else ''
            if len(resFeaturesContext) > 3:
              df.at[index, 'response_features_0_context_3_id']         = resFeaturesContext[3]['id'] if ('id' in resFeaturesContext[3]) else ''
              df.at[index, 'response_features_0_context_3_text']       = resFeaturesContext[3]['text'] if ('text' in resFeaturesContext[3]) else ''
              df.at[index, 'response_features_0_context_3_short_code'] = resFeaturesContext[3]['short_code'] if ('short_code' in resFeaturesContext[3]) else ''
            if len(resFeaturesContext) > 4:
              df.at[index, 'response_features_0_context_4_id']         = resFeaturesContext[4]['id'] if ('id' in resFeaturesContext[4]) else ''
              df.at[index, 'response_features_0_context_4_text']       = resFeaturesContext[4]['text'] if ('text' in resFeaturesContext[4]) else ''
              df.at[index, 'response_features_0_context_4_short_code'] = resFeaturesContext[4]['short_code'] if ('short_code' in resFeaturesContext[4]) else ''
      else:
        print(f"\033[93m {index+1}: RESPONSE 200: NO FEATURES")

    else:
      print(f"\033[91m {index+1}: Error en consulta, con status code: {response.status_code}")
    

    # Update the database row
    # update_cols = ', '.join([f"{col} = ?" for col in df.columns if col != 'index'])
    # sql = f"UPDATE postal SET {update_cols} WHERE index = ?"
    rowUpdated = df.loc[index].to_dict()
    conn = sqlite3.connect('postalCodes.sqlite3')
    cursor = conn.cursor()
    sql = "UPDATE postal SET http_status_code = ?, http_reason = ?, http_text = ?, response_features_0_place_type = ?, response_features_0_relevance = ?, response_features_0_text = ?, response_features_0_place_name = ?, response_features_0_center_longitude = ?, response_features_0_center_latitude = ?, response_features_0_context_0_id = ?, response_features_0_context_0_text = ?, response_features_0_context_0_short_code = ?, response_features_0_context_1_id = ?, response_features_0_context_1_text = ?, response_features_0_context_1_short_code = ?, response_features_0_context_2_id = ?, response_features_0_context_2_text = ?, response_features_0_context_2_short_code = ?, response_features_0_context_3_id = ?, response_features_0_context_3_text = ?, response_features_0_context_3_short_code = ?, response_features_0_context_4_id = ?, response_features_0_context_4_text = ?, response_features_0_context_4_short_code = ? WHERE id = ?"
    cursor.execute(sql, (rowUpdated['http_status_code'], rowUpdated['http_reason'], rowUpdated['http_text'], rowUpdated['response_features_0_place_type'], rowUpdated['response_features_0_relevance'], rowUpdated['response_features_0_text'], rowUpdated['response_features_0_place_name'], rowUpdated['response_features_0_center_longitude'], rowUpdated['response_features_0_center_latitude'], rowUpdated['response_features_0_context_0_id'], rowUpdated['response_features_0_context_0_text'], rowUpdated['response_features_0_context_0_short_code'], rowUpdated['response_features_0_context_1_id'], rowUpdated['response_features_0_context_1_text'], rowUpdated['response_features_0_context_1_short_code'], rowUpdated['response_features_0_context_2_id'], rowUpdated['response_features_0_context_2_text'], rowUpdated['response_features_0_context_2_short_code'], rowUpdated['response_features_0_context_3_id'], rowUpdated['response_features_0_context_3_text'], rowUpdated['response_features_0_context_3_short_code'], rowUpdated['response_features_0_context_4_id'], rowUpdated['response_features_0_context_4_text'], rowUpdated['response_features_0_context_4_short_code'], rowUpdated['id']) )
    conn.commit()
    conn.close()
    print("\033[92m ID updated in database: " + str(rowUpdated['id']))

  except Exception as e:
    print('\033[91m' + e)





# with open("output.csv",'a+', encoding='UTF8', newline='') as f:
#   writer = csv.writer(f, dialect='excel', delimiter='|')
#   writer.writerow( list(df.columns.values) )

# mpresult = fetch_geocode_data(query, mapbox_access_token, "MX", 1)
# print(mpresult)
# print(mpresult.json())
# print(mpresult.text)
# print(json.loads(mpresult.text))


# simulacion_reason = obj_pre['reason'] if ('reason' in obj_pre) else ''


















# yave_user = "eduardo.gutierrez+yave@truehome.com.mx"
# yave_pass = "aQ7?WN#NzM9Vu8kT"
# yave_sl_origin = "service_truehome"



# file_name = 'muestra.xlsx'
# excel_document = openpyxl.load_workbook(file_name)

# sheet = excel_document['pre_aprobadores_20210511']
# data_range = sheet.tables.items()[0][1]
# data = sheet[data_range]

# token = json.loads(authyave().text)['token']
# cdmx_state_code = 9

# for index, row in enumerate(data):
    
#     try:
    
#         rfc = row[13].value
#         score = int(row[7].value)
#         property_price = float(row[6].value)
#         down_payment = (float(row[6].value) - float(row[1].value))
#         bc_credit_history_xml = row[12].value
#         tax_scheme = row[14].value
#         income = float(row[3].value) + float(row[4].value)
#         income_frecuency = 'MONTHLY'
#         income_type = 'NET'
#         property_location = cdmx_state_code
#         bc_credit_history_xml = '<Cuentas>' + bc_credit_history_xml + '</Cuentas>'
#         down_payment = 0 if (down_payment < 0) else down_payment

#         simulacion_preaprobacion = simularpreaprobacionyave(token, rfc, score, property_price, down_payment, bc_credit_history_xml, tax_scheme, income, income_frecuency, income_type, property_location)
#         obj_pre = json.loads(simulacion_preaprobacion.text)

#         simulacion_accepted = obj_pre['accepted'] if ('accepted' in obj_pre) else ''
#         simulacion_max_loan = obj_pre['max_loan'] if ('max_loan' in obj_pre) else ''
#         simulacion_max_loan_payment = obj_pre['max_loan_payment'] if ('max_loan_payment' in obj_pre) else ''
#         simulacion_id = obj_pre['id'] if ('id' in obj_pre) else ''
#         simulacion_reason = obj_pre['reason'] if ('reason' in obj_pre) else ''

#         timestr = time.strftime("%Y%m%d-%H%M%S")
        
#         row[15].value = simulacion_accepted
#         row[16].value = simulacion_reason
#         row[17].value = simulacion_max_loan
#         row[18].value = simulacion_max_loan_payment
#         row[19].value = simulacion_id
#         row[20].value = str(obj_pre)
        
#         print(index, simulacion_accepted, simulacion_max_loan, simulacion_max_loan_payment, simulacion_id, simulacion_reason)

#     except Exception as e:
        
#         print(e)
        
# excel_document.save(path + timestr + '_' + file_name)
# excel_document.save(timestr + '_' + file_name)


# for idx, column in enumerate(data[0]):
#     print(idx, column.value)