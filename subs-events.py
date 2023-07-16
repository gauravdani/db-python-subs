# Databricks notebook source
# MAGIC %run ../lib/connectors

# COMMAND ----------

# MAGIC %run ../lib/helpers

# COMMAND ----------

# MAGIC %scala
# MAGIC // schedule job to run every morning 07:00 for the day before. The day before is in SQL, we just set the job
# MAGIC 
# MAGIC 
# MAGIC val dateNow = java.time.LocalDate.now
# MAGIC println(dateNow)
# MAGIC val query = """  SELECT
# MAGIC                         DISTINCT B.USER_ID,
# MAGIC                         A.EVENT_TYPE,
# MAGIC                         DATE (EVENT_NTZ,'Europe/Berlin') AS DATE_OF_CHANGE,
# MAGIC                         A.SUBSCRIPTION_ID,
# MAGIC                         A.OFFER_TYPE,
# MAGIC                         FIRST_VALUE(b.user_pseudo_id) OVER (PARTITION BY b.user_id ORDER BY CASE WHEN b.user_pseudo_id IS NULL THEN 0 ELSE 1 END DESC, b.event_timestamp) AS client_id, 
# MAGIC                         CASE
# MAGIC                           WHEN FIRST_VALUE(b.app_info.firebase_app_id) OVER (PARTITION BY b.user_id ORDER BY b.event_timestamp) IS NULL THEN 'G-WE9H82FYCK'
# MAGIC                         ELSE
# MAGIC                         FIRST_VALUE(b.app_info.firebase_app_id) OVER (PARTITION BY b.user_id ORDER BY b.event_timestamp)
# MAGIC                       END
# MAGIC                         AS app_id
# MAGIC                       FROM
# MAGIC                         `joyn-data-prd.snowflake_prd.subscription_event_f` A
# MAGIC                       INNER JOIN
# MAGIC                         `joyn-data-prd.analytics_283039314.events_*` B
# MAGIC                       ON
# MAGIC                         TO_HEX(SHA256(CONCAT('google|',A.user_id))) = B.user_id
# MAGIC                         AND FORMAT_DATE('%Y%m%d',DATE(EVENT_NTZ,'Europe/Berlin')) =  FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE('Europe/Berlin'),INTERVAL 1 day))
# MAGIC                       ORDER BY
# MAGIC                         1 desc
# MAGIC                      """
# MAGIC 
# MAGIC val snowflakeDF = queryBigQuery(query,options = Map(
# MAGIC      "parentProject" -> "joyn-data-prd",
# MAGIC      "project" -> "joyn-data-prd",
# MAGIC      "temporaryGcsBucket" -> "joyn-spark-bigquery-integration",
# MAGIC      "materializationDataset" -> "spark_temporary_tables_eu",
# MAGIC      "viewMaterializationDataset" -> "spark_temporary_tables_eu",
# MAGIC      "credentialsFile" -> "/dbfs/FileStore/shared_uploads/credentials/databricks_prd_joyn_data_prd.json"
# MAGIC    ))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(snowflakeDF)
# MAGIC snowflakeDF.createOrReplaceTempView("snowflakeDF")

# COMMAND ----------

pySnowflakeDF = spark.sql("""\


SELECT 
USER_ID as user_id,
case when EVENT_TYPE = 'Cancellation' then 'cancellation' 
     when EVENT_TYPE = 'Request' then 'subs_request'
	 when EVENT_TYPE = 'Renewal' then 'renewal'
	 when EVENT_TYPE = 'PaymentSuccessful' then 'payment_successful'
	 when EVENT_TYPE = 'PaymentFailed' then 'payment_failed'
	 when EVENT_TYPE = 'PaymentDelayedSuspension' then 'payment_delayed_suspension'
	 when EVENT_TYPE = 'PaymentDelayedGrace' then 'payment_delayed_grace'
	 when EVENT_TYPE = 'InvoiceCreated' then 'invoice_created'
	 when EVENT_TYPE = 'Creation' then 'creation'
	 when EVENT_TYPE = 'ChangePayment' then 'change_payment'
	 when EVENT_TYPE = 'InvoiceCreated' then 'invoice_created'
     else 'not available' end
 as event_custom_type,
DATE_OF_CHANGE as date_of_change,
CLIENT_ID as client_id,
APP_ID as app_id,
case 
WHEN a.app_id LIKE '%G-%' THEN concat('https://www.google-analytics.com/mp/collect?measurement_id=',a.app_id,'&api_secret=kYdrLEsgQXConKfP5PSK1g')
when a.app_id Like '%ios%' THEN concat('https://www.google-analytics.com/mp/collect?firebase_app_id=',a.app_id,'&api_secret=xzpxCkRxT2GVe8NV-CGiCA')
ELSE concat('https://www.google-analytics.com/mp/collect?firebase_app_id=',a.app_id,'&api_secret=H5AND3KmRJyypPgYwr5QfA') END AS mp_url,
a.SUBSCRIPTION_ID as subscription_id,
a.offer_type as offer_type
FROM snowflakeDF a

""") 

display(pySnowflakeDF)

# COMMAND ----------


subscription_data =pySnowflakeDF


# COMMAND ----------

import requests
import json
import pandas as pd
import numpy as np



# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("subs-events-main-app").getOrCreate()
subscription_data.createOrReplaceTempView("sub_events_db")
test_data = spark.sql('select * from sub_events_db') 
test_data=test_data.rdd
test_data.getNumPartitions()
count = test_data.count()
buckets = round(count / 29)

# COMMAND ----------

subsDF = spark.sql ("""\
              SELECT 
                  app_id,
                  bucket,
                  to_json(
                    collect_list(payload)    
                  ) as payload_array
              from 
                        (select ntile({0}) over (partition by a.app_id order by a.app_id) as bucket, 
                         a.app_id,
                         struct( 
                                 mp_url as URL, 
                                 a.app_id AS app_id, 
                                 a.client_id as client_id, 
                                 a.user_id as user_id,
                                 collect_list(struct(a.event_custom_type as name,
                                                      struct('true' as offline_event,
                                                             offer_type as offer_type,
                                                             subscription_id as subscription_id) as params)) over (partition by a.user_id) as  events) as payload 
                                 FROM sub_events_db a 
              ) 
              group by app_id,bucket
              """.format(buckets))

subsDF.first()
subsDF.count()
subsDFCount = subsDF.rdd.map(lambda x: len(json.loads(x[2]))).collect()
display(subsDF)

# COMMAND ----------

subsDFCount
len(subsDFCount)

# COMMAND ----------

import pyspark
import requests
from pyspark.sql import SparkSession
from requests import Request, Session

def sendHTTPRequesToGA4(K):
    s = Session()
    y = json.loads(K)
    sc=list(map(lambda x:runPayload(x,s),y))
    return (K,sc)

def testFunc(y,s):
    return y['URL'].find('ios')

def runPayload(y,s):
    url = y['URL']
    if(url.find('ios')>-1 or url.find('android')>-1):
        y["app_instance_id"]=y["client_id"]
        del y["client_id"]
    del y["app_id"]
    del y["URL"]
    y = json.dumps(y).encode('utf-8')
    l = str(len(y))
    r = Request('POST', url, data=y, headers={'Content-Type': 'application/json','Content-Length':l})
    p = r.prepare()
    resp = s.send(p)
    return resp.status_code
  
#sendHTTPRequesToGA4(sample_data)

# COMMAND ----------

def sendNewHTTPRequesToGA4(y,s):
  x=  json.loads(y)
  s = requests.Session()
  a = requests.adapters.HTTPAdapter(pool_connections=100,pool_maxsize=100)
  session.mount('http://', adapter)  
  
def newfunc(partition):  
  return partition

#subsRDD = subsDF.rdd.repartition(10).mapPartitions(lambda x: x,preservesPartitioning=True)

subsRDD = subsDF.rdd.repartition(10).map(lambda x: sendHTTPRequesToGA4(x[2]))

subsRDD.take(20)
subsRDD.count()
  

# COMMAND ----------


  
subsRDD.take(20)



