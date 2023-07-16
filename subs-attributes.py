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
# MAGIC val query = """ WITH subscription_data AS ( WITH latest_entry AS
# MAGIC                         (
# MAGIC                                  SELECT   user_id,
# MAGIC                                           Max(calendar_date) AS calendar_date
# MAGIC                                  FROM     `joyn-data-prd.snowflake_prd.subscription_snapshot_f`
# MAGIC                                  WHERE    base_group != 'churn'
# MAGIC                                  AND      calendar_date >= date_add(CURRENT_DATE('Europe/Berlin'), interval -7 day)
# MAGIC                                  GROUP BY 1 ), churn AS
# MAGIC                         (
# MAGIC                                  SELECT   user_id,
# MAGIC                                           subscription_start_date,
# MAGIC                                           max(billing_cycle_end_date) AS billing_cycle_end_date,
# MAGIC                                           max(churn_type)             AS churn_type,
# MAGIC                                           max(
# MAGIC                                           CASE
# MAGIC                                                    WHEN churn_type = 'involuntary' THEN date(calendar_date)
# MAGIC                                                    ELSE                                 date(billing_cycle_end_date)
# MAGIC                                           END) AS subscription_end_date,
# MAGIC                                  FROM     `joyn-data-prd.snowflake_prd.subscription_snapshot_f` sb
# MAGIC                                  WHERE    base_group = 'churn'
# MAGIC                                  GROUP BY user_id,
# MAGIC                                           subscription_start_date)
# MAGIC                         SELECT DISTINCT sb.user_id,
# MAGIC                                         date(sb.subscription_start_date)                                                               AS subscription_start_date,
# MAGIC                                         date(sb.subscription_cancel_dts)                                                               AS subscription_cancel_date,
# MAGIC                                         date(COALESCE(sb.billing_cycle_start_date,'1900-01-01'))                                       AS subscription_next_renewal_date,
# MAGIC                                         date(COALESCE(churn.subscription_end_date,date(sb.billing_cycle_end_date),date('1900-01-01'))) AS subscription_end_date,
# MAGIC                                         cast(sb.subscription_month AS int64)                                                           AS subscription_month,
# MAGIC                                         sb.gift_card_code                                                                              AS gift_card_code,
# MAGIC                                         COALESCE(sb.payment_vendor, 'unknown')                                                         AS subscription_payment_vendor,
# MAGIC                                         CASE
# MAGIC                                                         WHEN churn.churn_type = 'voluntary'
# MAGIC                                                         AND             churn.subscription_end_date < CURRENT_DATE() THEN 'churn voluntary'
# MAGIC                                                         WHEN churn.churn_type = 'involuntary'
# MAGIC                                                         AND             churn.subscription_end_date < CURRENT_DATE()THEN 'churn involuntary'
# MAGIC                                                         WHEN sb.calendar_date < date_add( CURRENT_DATE(('Europe/Berlin')), interval -1 day) THEN 'churn unknown'
# MAGIC                                                         WHEN date(sb.subscription_cancel_dts) < CURRENT_DATE(('Europe/Berlin'))
# MAGIC                                                         AND             CURRENT_DATE() < COALESCE(churn.subscription_end_date, '2199-12-31') THEN 'in cancelation period'
# MAGIC                                                         WHEN churn.subscription_end_date IS NULL
# MAGIC                                                         OR              churn.subscription_end_date >= CURRENT_DATE(('Europe/Berlin')) THEN 'active'
# MAGIC                                                         ELSE 'unknown'
# MAGIC                                         END        AS subscription_status,
# MAGIC                                         sb.user_id AS bucket
# MAGIC                         FROM            `joyn-data-prd.snowflake_prd.subscription_snapshot_f` sb
# MAGIC                         JOIN            latest_entry
# MAGIC                         ON              sb.user_id = latest_entry.user_id
# MAGIC                         AND             sb.calendar_date = latest_entry.calendar_date
# MAGIC                         LEFT JOIN       churn
# MAGIC                         ON              sb.user_id = churn.user_id
# MAGIC                         AND             churn.subscription_end_date >= sb.subscription_start_date
# MAGIC                         AND             churn.billing_cycle_end_date = sb.billing_cycle_end_date
# MAGIC                         WHERE           sb.base_group != 'churn'
# MAGIC                         AND             sb.user_id NOT LIKE 'dummy%'
# MAGIC                         AND             (
# MAGIC                                                         sb.calendar_date <= COALESCE(churn.subscription_end_date, '2199-12-31')
# MAGIC                                         OR              sb.subscription_start_date > churn.subscription_end_date))
# MAGIC                         SELECT DISTINCT a.subscription_cancel_date as SUBSCRIPTION_CANCEL_DATE,
# MAGIC                                         a.subscription_next_renewal_date as SUBSCRIPTION_RENEWAL_DATE,
# MAGIC                                         a.subscription_start_date AS SUBSCRIPTION_START_DATE,
# MAGIC                                         a.subscription_end_date AS SUBSCRIPTION_END_DATE,
# MAGIC                                         a.subscription_status AS SUBSCRIPTION_STATUS,
# MAGIC                                         a.user_id                                                                              AS jnde_user_id,
# MAGIC                                         to_hex(sha256(concat('google|',a.user_id)))                                            AS encrypted_user_id,
# MAGIC                                         b.user_id                                                                              AS ga_user_id,
# MAGIC                                         first_value(b.user_pseudo_id) OVER (partition BY b.user_id ORDER BY CASE WHEN b.user_pseudo_id IS NULL then 0 ELSE 1 END DESC, b.event_timestamp) AS client_id,
# MAGIC                                         CASE
# MAGIC                                                         WHEN first_value(b.app_info.firebase_app_id) OVER (partition BY b.user_id ORDER BY b.event_timestamp) IS NULL THEN 'G-WE9H82FYCK'
# MAGIC                                                         ELSE first_value(b.app_info.firebase_app_id) OVER (partition BY b.user_id ORDER BY b.event_timestamp)
# MAGIC                                         END AS app_id
# MAGIC                         FROM            subscription_data a
# MAGIC                         INNER JOIN      `joyn-data-prd.analytics_283039314.events_*` b
# MAGIC                         ON              to_hex(sha256(concat('google|',a.user_id))) = b.user_id 
# MAGIC                         and             _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Europe/Berlin'), INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE('Europe/Berlin'))
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
IF(Isnull(a.subscription_cancel_date),'',a.subscription_cancel_date) as subscription_cancel_date,
IF(isnull(a.client_id),'',a.client_id) AS client_id,cast(a.subscription_renewal_date AS varchar(10)) AS subscription_renewal_date , cast(a.subscription_start_date AS varchar(10)),cast(a.subscription_end_date AS varchar(10)),a.ga_user_id,a.app_id,a.subscription_status,
CASE
WHEN a.app_id LIKE '%G-%' THEN
  'kYdrLEsgQXConKfP5PSK1g'
WHEN a.app_id LIKE '%ios%' THEN
  'xzpxCkRxT2GVe8NV-CGiCA'
  ELSE 'H5AND3KmRJyypPgYwr5QfA'
END
AS app_secret,
CASE
WHEN a.app_id LIKE '%G-%' THEN concat('https://www.google-analytics.com/mp/collect?measurement_id=',a.app_id,'&api_secret=kYdrLEsgQXConKfP5PSK1g')
when a.app_id Like '%ios%' THEN concat('https://www.google-analytics.com/mp/collect?firebase_app_id=',a.app_id,'&api_secret=xzpxCkRxT2GVe8NV-CGiCA')
ELSE concat('https://www.google-analytics.com/mp/collect?firebase_app_id=',a.app_id,'&api_secret=H5AND3KmRJyypPgYwr5QfA') END AS mp_url 
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
spark = SparkSession.builder.appName("subs-status-main-app").getOrCreate()
subscription_data.createOrReplaceTempView("sub_dataset")
test_data = spark.sql('select * from sub_dataset') 
test_data=test_data.rdd
test_data.getNumPartitions()
count = test_data.count()
buckets = round(count / 29)


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
                                 a.ga_user_id as user_id,
                                 struct ( struct (a.subscription_status AS value) AS subscription_status, 
                                          struct (a.subscription_start_date AS value) AS subscription_start_date, 
                                          struct (a.subscription_end_date AS value) AS subscription_end_date, 
                                          struct (a.subscription_cancel_date AS value) AS subscription_cancel_date, 
                                          struct (a.subscription_renewal_date AS value) AS subscription_renewal_date ) AS user_properties,
                                          collect_list(struct('update_subs_state_v1' as name, 
                                                             struct('true' as offline_event) as params)) over (partition by a.ga_user_id) as  events) as payload 
                                 FROM sub_dataset a 
              ) 
              group by app_id,bucket
              """.format(buckets))

subsDF.first()
subsDF.count()
subsDFCount = subsDF.rdd.map(lambda x: len(json.loads(x[2]))).collect()


# COMMAND ----------

import pyspark
import requests
from pyspark.sql import SparkSession
from requests import Request, Session

def sendHTTPRequesToGA4(K):
    s = Session()
    y = json.loads(K)
    sc=list(map(lambda x:runPayload(x,s),y))
    return sc

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

import requests
import json
import pandas as pd
import numpy as np

"""
sample_data = '[{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"A0DE642266AD46CC846B06F73863932D","user_id":"00040814c958fd86f71ae474a6bffc3120cc91a888783b69096915c1fa107a15","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-09-22 00:00:00"},"subscription_end_date":{"value":"2022-11-22 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-22 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"32A0A10CEF814D8C877A8FD51407931C","user_id":"0016b86edb1da81d5eedeb531b95df1e100070a1da77e8f4b7c10fdf20d94c07","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-09-10 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-02 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E9E99609AC2F4D79A6C55CB53FA751F0","user_id":"00246c75f85d7c3302931dd746862c88059aea8e644884fd807b45807c54694b","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-11-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E324A2315F8142E78BDDC25450DCB9FE","user_id":"002948234957eaa3a14d4585af183d8b8ef5e0fbe856e57b0fdeb7c91ad75965","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2021-01-16 00:00:00"},"subscription_end_date":{"value":"2022-11-14 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-14 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"CA4DA074A81D4D139FF2B10B3FEADC02","user_id":"003c4b5116b12b4529273f47e722b7a6df18fa5866798ad13cbc4f25277bae66","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-07-16 00:00:00"},"subscription_end_date":{"value":"2022-11-15 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-16 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"6699464356544C4AAD2CF108A09A72C2","user_id":"004020a4685b8a01e2e660ad4ee46d305b9d9511172d7a15e922518f2626973f","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-04-12 00:00:00"},"subscription_end_date":{"value":"2022-11-12 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-12 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"D17B36FBD0424AEDAB3F0885C798997C","user_id":"004053fd1d5500e1d41b2d418be92189b81ece0c14bbf8211a1985ad34438e05","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-08-25 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-01 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"28A1A5901FF64C37981C4A91649550DD","user_id":"004efff5df56fcdb2fa24df331b720e906c854e38b6acc44cb2960a364ba5b59","user_properties":{"subscription_status":{"value":"in cancelation period"},"subscription_start_date":{"value":"2022-10-21 00:00:00"},"subscription_end_date":{"value":"2022-10-28 00:00:00"},"subscription_cancel_date":{"value":"2022-10-21 00:00:00"},"subscription_renewal_date":{"value":"2022-10-21 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"4D584A3D9AA943558FBA3246ECB0C985","user_id":"00723758fff13c9cf186ef69c1d1ea759f5631def167831c7dd75d41ad2bd23e","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-08-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"A0DE642266AD46CC846B06F73863932D","user_id":"00040814c958fd86f71ae474a6bffc3120cc91a888783b69096915c1fa107a15","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-09-22 00:00:00"},"subscription_end_date":{"value":"2022-11-22 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-22 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"32A0A10CEF814D8C877A8FD51407931C","user_id":"0016b86edb1da81d5eedeb531b95df1e100070a1da77e8f4b7c10fdf20d94c07","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-09-10 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-02 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E9E99609AC2F4D79A6C55CB53FA751F0","user_id":"00246c75f85d7c3302931dd746862c88059aea8e644884fd807b45807c54694b","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-11-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E324A2315F8142E78BDDC25450DCB9FE","user_id":"002948234957eaa3a14d4585af183d8b8ef5e0fbe856e57b0fdeb7c91ad75965","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2021-01-16 00:00:00"},"subscription_end_date":{"value":"2022-11-14 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-14 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"CA4DA074A81D4D139FF2B10B3FEADC02","user_id":"003c4b5116b12b4529273f47e722b7a6df18fa5866798ad13cbc4f25277bae66","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-07-16 00:00:00"},"subscription_end_date":{"value":"2022-11-15 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-16 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"6699464356544C4AAD2CF108A09A72C2","user_id":"004020a4685b8a01e2e660ad4ee46d305b9d9511172d7a15e922518f2626973f","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-04-12 00:00:00"},"subscription_end_date":{"value":"2022-11-12 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-12 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"D17B36FBD0424AEDAB3F0885C798997C","user_id":"004053fd1d5500e1d41b2d418be92189b81ece0c14bbf8211a1985ad34438e05","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-08-25 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-01 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"28A1A5901FF64C37981C4A91649550DD","user_id":"004efff5df56fcdb2fa24df331b720e906c854e38b6acc44cb2960a364ba5b59","user_properties":{"subscription_status":{"value":"in cancelation period"},"subscription_start_date":{"value":"2022-10-21 00:00:00"},"subscription_end_date":{"value":"2022-10-28 00:00:00"},"subscription_cancel_date":{"value":"2022-10-21 00:00:00"},"subscription_renewal_date":{"value":"2022-10-21 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"4D584A3D9AA943558FBA3246ECB0C985","user_id":"00723758fff13c9cf186ef69c1d1ea759f5631def167831c7dd75d41ad2bd23e","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-08-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"A0DE642266AD46CC846B06F73863932D","user_id":"00040814c958fd86f71ae474a6bffc3120cc91a888783b69096915c1fa107a15","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-09-22 00:00:00"},"subscription_end_date":{"value":"2022-11-22 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-22 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"32A0A10CEF814D8C877A8FD51407931C","user_id":"0016b86edb1da81d5eedeb531b95df1e100070a1da77e8f4b7c10fdf20d94c07","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-09-10 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-02 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E9E99609AC2F4D79A6C55CB53FA751F0","user_id":"00246c75f85d7c3302931dd746862c88059aea8e644884fd807b45807c54694b","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-11-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E324A2315F8142E78BDDC25450DCB9FE","user_id":"002948234957eaa3a14d4585af183d8b8ef5e0fbe856e57b0fdeb7c91ad75965","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2021-01-16 00:00:00"},"subscription_end_date":{"value":"2022-11-14 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-14 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"CA4DA074A81D4D139FF2B10B3FEADC02","user_id":"003c4b5116b12b4529273f47e722b7a6df18fa5866798ad13cbc4f25277bae66","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-07-16 00:00:00"},"subscription_end_date":{"value":"2022-11-15 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-16 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"6699464356544C4AAD2CF108A09A72C2","user_id":"004020a4685b8a01e2e660ad4ee46d305b9d9511172d7a15e922518f2626973f","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-04-12 00:00:00"},"subscription_end_date":{"value":"2022-11-12 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-12 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"D17B36FBD0424AEDAB3F0885C798997C","user_id":"004053fd1d5500e1d41b2d418be92189b81ece0c14bbf8211a1985ad34438e05","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-08-25 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-01 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"28A1A5901FF64C37981C4A91649550DD","user_id":"004efff5df56fcdb2fa24df331b720e906c854e38b6acc44cb2960a364ba5b59","user_properties":{"subscription_status":{"value":"in cancelation period"},"subscription_start_date":{"value":"2022-10-21 00:00:00"},"subscription_end_date":{"value":"2022-10-28 00:00:00"},"subscription_cancel_date":{"value":"2022-10-21 00:00:00"},"subscription_renewal_date":{"value":"2022-10-21 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"4D584A3D9AA943558FBA3246ECB0C985","user_id":"00723758fff13c9cf186ef69c1d1ea759f5631def167831c7dd75d41ad2bd23e","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-08-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"A0DE642266AD46CC846B06F73863932D","user_id":"00040814c958fd86f71ae474a6bffc3120cc91a888783b69096915c1fa107a15","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-09-22 00:00:00"},"subscription_end_date":{"value":"2022-11-22 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-22 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"32A0A10CEF814D8C877A8FD51407931C","user_id":"0016b86edb1da81d5eedeb531b95df1e100070a1da77e8f4b7c10fdf20d94c07","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-09-10 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-02 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E9E99609AC2F4D79A6C55CB53FA751F0","user_id":"00246c75f85d7c3302931dd746862c88059aea8e644884fd807b45807c54694b","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-11-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"E324A2315F8142E78BDDC25450DCB9FE","user_id":"002948234957eaa3a14d4585af183d8b8ef5e0fbe856e57b0fdeb7c91ad75965","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2021-01-16 00:00:00"},"subscription_end_date":{"value":"2022-11-14 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-14 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"CA4DA074A81D4D139FF2B10B3FEADC02","user_id":"003c4b5116b12b4529273f47e722b7a6df18fa5866798ad13cbc4f25277bae66","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-07-16 00:00:00"},"subscription_end_date":{"value":"2022-11-15 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-16 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"6699464356544C4AAD2CF108A09A72C2","user_id":"004020a4685b8a01e2e660ad4ee46d305b9d9511172d7a15e922518f2626973f","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-04-12 00:00:00"},"subscription_end_date":{"value":"2022-11-12 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-12 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"D17B36FBD0424AEDAB3F0885C798997C","user_id":"004053fd1d5500e1d41b2d418be92189b81ece0c14bbf8211a1985ad34438e05","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2022-08-25 00:00:00"},"subscription_end_date":{"value":"2022-11-01 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-01 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"28A1A5901FF64C37981C4A91649550DD","user_id":"004efff5df56fcdb2fa24df331b720e906c854e38b6acc44cb2960a364ba5b59","user_properties":{"subscription_status":{"value":"in cancelation period"},"subscription_start_date":{"value":"2022-10-21 00:00:00"},"subscription_end_date":{"value":"2022-10-28 00:00:00"},"subscription_cancel_date":{"value":"2022-10-21 00:00:00"},"subscription_renewal_date":{"value":"2022-10-21 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]},{"URL":"https://www.google-analytics.com/mp/collect?firebase_app_id=1:714399944626:ios:dc39c3cabdcff0fe541eab&api_secret=xzpxCkRxT2GVe8NV","app_id":"1:714399944626:ios:dc39c3cabdcff0fe541eab","client_id":"4D584A3D9AA943558FBA3246ECB0C985","user_id":"00723758fff13c9cf186ef69c1d1ea759f5631def167831c7dd75d41ad2bd23e","user_properties":{"subscription_status":{"value":"active"},"subscription_start_date":{"value":"2020-08-19 00:00:00"},"subscription_end_date":{"value":"2022-11-18 00:00:00"},"subscription_cancel_date":{"value":""},"subscription_renewal_date":{"value":"2022-10-19 00:00:00"}},"events":[{"name":"update_subs_state_t2"}]}]'
"""

# COMMAND ----------


