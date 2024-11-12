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
