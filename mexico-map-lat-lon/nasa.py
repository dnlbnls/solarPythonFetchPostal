import json
import requests
import pandas as pd
import csv
import time
import sqlite3
import aiohttp
import asyncio


async def fetch(session, url, params=None, headers=None, context=None):
  try:
    async with session.get(url, params=params, headers=headers) as response:
      content = await response.text()
      response.raise_for_status()
      print(f"\033[95m {response.status}: {response.reason}")
      return { "status": response.status, "reason": response.reason, "text": content, "context": context }

  except aiohttp.ClientResponseError as e:
    print(f"\033[91m Error for URL {url}: {e}")
    return None
  except Exception as e:
    print(f"\033[91m Error occurred: {e}")
    return None

async def fetch_all(batch):
  async with aiohttp.ClientSession() as session:
    tasks = [fetch(session, data["url"], data["params"], data["headers"], data["context"] ) for data in batch]
    return await asyncio.gather(*tasks)

def chunks(lst, n):
  """Yield successive n-sized chunks from lst."""
  for i in range(0, len(lst), n):
    yield lst[i:i + n]


async def main():
 
  # Get data from sqlite database, order by relevance of postalCode
  conn = sqlite3.connect('postalCodesLonLatMapboxQuery.sqlite3')
  query = "SELECT * FROM postalMapBoxQuery WHERE (nasa_http_status_code = '' OR nasa_http_status_code != '200') AND (response_features_0_center_latitude IS NOT NULL AND response_features_0_center_longitude IS NOT NULL) ORDER BY postalCode_relevance_group ASC NULLS LAST"
  postal_df = pd.read_sql(query, conn)
  conn.close()

  requests_data = []

  print("\033[92m Preparing Request Data To Batch")
  for index, row in postal_df.iterrows():
    url = "https://power.larc.nasa.gov/api/temporal/climatology/point?parameters=SI_EF_TILTED_SURFACE,T2M_MAX,T2M_MIN,EQUIV_NO_SUN_CONSEC_MONTH,T2M&community=RE&format=JSON"
    params = {'longitude': row["response_features_0_center_longitude"], 'latitude': row["response_features_0_center_latitude"]}
    params = {k: v for k, v in params.items() if v is not None}
    headers = {'Accept': 'application/json'}
    requests_data.append({"url": url, "params": params, "headers": headers, "context": {"id":row["id"], "codigoPostal":row["codigoPostal"], "longitude":row["response_features_0_center_longitude"],"latitude":row["response_features_0_center_latitude"]} })

  for batch in chunks(requests_data, 20):  # Fetch X URLs at a time
    print("\033[92m ########### STARTING BATCH")
    # Fetch batch
    results = await fetch_all(batch)
    # Batch results  in database
    print("\033[92m ########### SAVING BATCH RESULTS TO DATABASE")
    for result in results:

      conn = sqlite3.connect('postalCodesLonLatMapboxQuery.sqlite3')
      cursor = conn.cursor()
      sql = "UPDATE postalMapBoxQuery SET nasa_http_status_code = ?, nasa_http_reason = ?, nasa_http_text = ? WHERE id = ?"
      cursor.execute(sql, (result["status"], result["reason"], result["text"] if ("text" in result) else '', result["context"]["id"]) )
      conn.commit()

      if "text" in result:
        res = json.loads(result["text"])
      else:
        res = []

      if "properties" in res:
        if "parameter" in res["properties"]:
          rowsToInsert = []
          for key, value in res["properties"]["parameter"].items():
            for dimension, value in value.items():
              value_type = "numeric" if isinstance(value, (int, float)) else ("string " if isinstance(value, str) else "ND" )
              numeric_value = value if isinstance(value, (int, float)) else None
              string_value = value if isinstance(value, str) else None
              rowsToInsert.append( (result["context"]["id"], result["context"]["codigoPostal"], key, dimension, value_type, numeric_value, string_value) )
          sql = "INSERT INTO postalNasa (postalMapBoxQuery_id, codigoPostal, variable, dimension, value_type, numeric_value, string_value) VALUES (?, ?, ?, ?, ?, ?, ?)"
          cursor.executemany(sql, rowsToInsert)
          conn.commit()

      conn.close()






asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())












# async def fetch_bored(query, access_token, country=None, limit=None):
#   # Mapbox Geocoding API Endpoint
#   url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"

#   # Define the parameters
#   params = {
#       'access_token': access_token,
#       'country': country,  # If specified, restricts search to a specific country
#       'limit': limit,       # If specified, limits the number of results returned
#       'types': 'postcode'
#   }

#   # Remove None values from parameters
#   params = {k: v for k, v in params.items() if v is not None}

#   headers = {
#       'Accept': 'application/json'
#   }

#   try:
#     response = requests.get(url, params=params, headers=headers)
#     if response.status_code == 200:
#       return response
#     elif response.status_code == 401:
#       print (f"Unauthorized - check your access token: {response.status_code}")
#       return response
#     elif response.status_code == 403:
#       print (f"Forbidden - access token may not have proper permissions: {response.status_code}")
#       return response
#     elif response.status_code == 404:
#       print (f"Location not found: {response.status_code}")
#       return response
#     elif response.status_code == 429:
#       print (f"Too many requests - rate limit exceeded: {response.status_code}")
#       return response
#     elif response.status_code > 429:
#       print(f"Your request has failed with status code: {response.status_code}")
#       return response
#     else:
#       response.raise_for_status()
#       return response
#   except requests.exceptions.ConnectionError as error:
#     print (f"Request error: {error}")
#     return



# # Codigo para consultar Mapbox
# def fetch_nasa_data(query, access_token, country=None, limit=None):
#   # Mapbox Geocoding API Endpoint
#   url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"

#   # Define the parameters
#   params = {
#       'access_token': access_token,
#       'country': country,  # If specified, restricts search to a specific country
#       'limit': limit,       # If specified, limits the number of results returned
#       'types': 'postcode'
#   }

#   # Remove None values from parameters
#   params = {k: v for k, v in params.items() if v is not None}

#   headers = {
#       'Accept': 'application/json'
#   }

#   try:
#     response = requests.get(url, params=params, headers=headers)
#     if response.status_code == 200:
#       return response
#     elif response.status_code == 401:
#       print (f"Unauthorized - check your access token: {response.status_code}")
#       return response
#     elif response.status_code == 403:
#       print (f"Forbidden - access token may not have proper permissions: {response.status_code}")
#       return response
#     elif response.status_code == 404:
#       print (f"Location not found: {response.status_code}")
#       return response
#     elif response.status_code == 429:
#       print (f"Too many requests - rate limit exceeded: {response.status_code}")
#       return response
#     elif response.status_code > 429:
#       print(f"Your request has failed with status code: {response.status_code}")
#       return response
#     else:
#       response.raise_for_status()
#       return response
#   except requests.exceptions.ConnectionError as error:
#     print (f"Request error: {error}")
#     return



# conn = sqlite3.connect('postalCodes.sqlite3')
# query = "SELECT * FROM postal WHERE http_status_code = '' OR http_status_code != '200'"
# df = pd.read_sql(query, conn).sample(frac=1).reset_index(drop=True)
# conn.close()



# for index, row in df.iterrows():
#   try:

#     response = fetch_geocode_data(row['mb_query'], mapbox_access_token, "MX", 1)
#     time.sleep(0.100)

#     df.at[index, 'http_status_code'] = getattr(response, 'status_code', '')
#     df.at[index, 'http_reason'] = getattr(response, 'reason', '')
#     df.at[index, 'http_text'] = getattr(response, 'text', '')

#     if response.status_code == 200:
#       res = json.loads(response.text)

#       if "features" in res:
#         if len(res["features"]) > 0:

#           print(f"\033[92m{index+1}: RESPONSE 200: FEATURES INCLUDED. SAVING IN DATAFRAME")

#           resFeatures = res["features"][0]
#           df.at[index, 'response_features_0_place_type']           = resFeatures['place_type'][0] if ('place_type' in resFeatures) else ''
#           df.at[index, 'response_features_0_relevance']            = resFeatures['relevance'] if ('relevance' in resFeatures) else ''
#           df.at[index, 'response_features_0_text']                 = resFeatures['text'] if ('text' in resFeatures) else ''
#           df.at[index, 'response_features_0_place_name']           = resFeatures['place_name'] if ('place_name' in resFeatures) else ''
#           df.at[index, 'response_features_0_center_longitude']     = resFeatures['center'][0] if ('center' in resFeatures) else ''
#           df.at[index, 'response_features_0_center_latitude']      = resFeatures['center'][1] if ('center' in resFeatures) else ''
#           if("context" in resFeatures):
#             resFeaturesContext = resFeatures['context']
#             if len(resFeaturesContext) > 0:
#               df.at[index, 'response_features_0_context_0_id']         = resFeaturesContext[0]['id'] if ('id' in resFeaturesContext[0]) else ''
#               df.at[index, 'response_features_0_context_0_text']       = resFeaturesContext[0]['text'] if ('text' in resFeaturesContext[0]) else ''
#               df.at[index, 'response_features_0_context_0_short_code'] = resFeaturesContext[0]['short_code'] if ('short_code' in resFeaturesContext[0]) else ''
#             if len(resFeaturesContext) > 1:
#               df.at[index, 'response_features_0_context_1_id']         = resFeaturesContext[1]['id'] if ('id' in resFeaturesContext[1]) else ''
#               df.at[index, 'response_features_0_context_1_text']       = resFeaturesContext[1]['text'] if ('text' in resFeaturesContext[1]) else ''
#               df.at[index, 'response_features_0_context_1_short_code'] = resFeaturesContext[1]['short_code'] if ('short_code' in resFeaturesContext[1]) else ''
#             if len(resFeaturesContext) > 2:
#               df.at[index, 'response_features_0_context_2_id']         = resFeaturesContext[2]['id'] if ('id' in resFeaturesContext[2]) else ''
#               df.at[index, 'response_features_0_context_2_text']       = resFeaturesContext[2]['text'] if ('text' in resFeaturesContext[2]) else ''
#               df.at[index, 'response_features_0_context_2_short_code'] = resFeaturesContext[2]['short_code'] if ('short_code' in resFeaturesContext[2]) else ''
#             if len(resFeaturesContext) > 3:
#               df.at[index, 'response_features_0_context_3_id']         = resFeaturesContext[3]['id'] if ('id' in resFeaturesContext[3]) else ''
#               df.at[index, 'response_features_0_context_3_text']       = resFeaturesContext[3]['text'] if ('text' in resFeaturesContext[3]) else ''
#               df.at[index, 'response_features_0_context_3_short_code'] = resFeaturesContext[3]['short_code'] if ('short_code' in resFeaturesContext[3]) else ''
#             if len(resFeaturesContext) > 4:
#               df.at[index, 'response_features_0_context_4_id']         = resFeaturesContext[4]['id'] if ('id' in resFeaturesContext[4]) else ''
#               df.at[index, 'response_features_0_context_4_text']       = resFeaturesContext[4]['text'] if ('text' in resFeaturesContext[4]) else ''
#               df.at[index, 'response_features_0_context_4_short_code'] = resFeaturesContext[4]['short_code'] if ('short_code' in resFeaturesContext[4]) else ''
#       else:
#         print(f"\033[93m {index+1}: RESPONSE 200: NO FEATURES")

#     else:
#       print(f"\033[91m {index+1}: Error en consulta, con status code: {response.status_code}")
    

#     rowUpdated = df.loc[index].to_dict()
#     conn = sqlite3.connect('postalCodes.sqlite3')
#     cursor = conn.cursor()
#     sql = "UPDATE postal SET http_status_code = ?, http_reason = ?, http_text = ?, response_features_0_place_type = ?, response_features_0_relevance = ?, response_features_0_text = ?, response_features_0_place_name = ?, response_features_0_center_longitude = ?, response_features_0_center_latitude = ?, response_features_0_context_0_id = ?, response_features_0_context_0_text = ?, response_features_0_context_0_short_code = ?, response_features_0_context_1_id = ?, response_features_0_context_1_text = ?, response_features_0_context_1_short_code = ?, response_features_0_context_2_id = ?, response_features_0_context_2_text = ?, response_features_0_context_2_short_code = ?, response_features_0_context_3_id = ?, response_features_0_context_3_text = ?, response_features_0_context_3_short_code = ?, response_features_0_context_4_id = ?, response_features_0_context_4_text = ?, response_features_0_context_4_short_code = ? WHERE id = ?"
#     cursor.execute(sql, (rowUpdated['http_status_code'], rowUpdated['http_reason'], rowUpdated['http_text'], rowUpdated['response_features_0_place_type'], rowUpdated['response_features_0_relevance'], rowUpdated['response_features_0_text'], rowUpdated['response_features_0_place_name'], rowUpdated['response_features_0_center_longitude'], rowUpdated['response_features_0_center_latitude'], rowUpdated['response_features_0_context_0_id'], rowUpdated['response_features_0_context_0_text'], rowUpdated['response_features_0_context_0_short_code'], rowUpdated['response_features_0_context_1_id'], rowUpdated['response_features_0_context_1_text'], rowUpdated['response_features_0_context_1_short_code'], rowUpdated['response_features_0_context_2_id'], rowUpdated['response_features_0_context_2_text'], rowUpdated['response_features_0_context_2_short_code'], rowUpdated['response_features_0_context_3_id'], rowUpdated['response_features_0_context_3_text'], rowUpdated['response_features_0_context_3_short_code'], rowUpdated['response_features_0_context_4_id'], rowUpdated['response_features_0_context_4_text'], rowUpdated['response_features_0_context_4_short_code'], rowUpdated['id']) )
#     conn.commit()
#     conn.close()
#     print("\033[92m ID updated in database: " + str(rowUpdated['id']))

#   except Exception as e:
#     print('\033[91m' + e)
