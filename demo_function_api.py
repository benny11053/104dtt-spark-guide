#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import pandas as pd
import pyspark
import datetime

import time
import calendar
import time


from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col,input_file_name
from pyspark.sql.types import IntegerType, DateType, LongType, TimestampType


# In[14]:


spark.stop()


# In[3]:


start_time = datetime.datetime.now()
#spark engine executing
spark = (SparkSession
              .builder\
              .master("local[*]")\
              .appName("spark_performace")\
              .config("spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem")\
              .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")\
              .config("spark.hadoop.fs.s3a.endpoint", "s3-ap-northeast-1.amazonaws.com")\
              .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
              .config("spark.sql.execution.arrow.pyspark.enabled","true")\
              .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled","true")\
              .config("spark.debug.maxToStringFields", 2000)\
              .config("spark.sql.broadcastTimeout",  36000)\
              .config("spark.sql.shuffle.partitions", 128)\
              .config("spark.default.parallelism", 16)\
              .getOrCreate())


# In[4]:


sample_url = "s3a://aws-test-benny/assessment/target_user_sample/*.parquet"
basic_url = "s3a://aws-test-benny/assessment/basic/*.parquet"
exp_url = "s3a://aws-test-benny/assessment/exp_job/*.parquet"
cid_url = "s3a://aws-test-benny/assessment/cid_mapping/*.parquet"

sample_df= spark     .read     .parquet(sample_url)

basic_df= spark     .read     .parquet(basic_url)

exp_df = spark     .read     .parquet(exp_url)

cid_df = spark     .read     .parquet(cid_url)

#sample_df.printSchema()

#basic_df.printSchema()

#exp_df.printSchema()

#cid_df.printSchema()

# exp JOIN basic ON idno JOIN sample_df ON idno
# job_cat_no>0 or ind_cat_no>0

join_df = exp_df .join(basic_df,["id_no"]) .join(sample_df, exp_df.id_no == sample_df.idno,'inner') .where((f.col("job_cat_no")>0) | (f.col("ind_cat_no")>0))


# In[5]:


# first_transform_df
# 取代 enddate
# groupby pkey 取代 difftime drop duplicates
# difftime = abs(((end_date-start_date)/2+start_date)-finish_date)
# sm=  (end_y-start_y)*12+(end_m-start_m)
# sy={
#      sm=0 , 2+log(0.5/3)/log(2);
#      o.w. ,  2+log(sm/3)/log(2)
#         }
replace_condition = (f.datediff(f.col("end_date"),f.col("start_date"))<0)
middle_date_diff = f.datediff(f.col("end_date"),f.col("start_date"))/2
update_date_diff = f.datediff(f.col("start_date"),f.col("finish_date"))
sm = (f.col("end_y") - f.col("start_y")) * 12 + (f.col("end_m") - f.col("start_m"))
sy_default = f.lit(2)+f.lit(f.log(f.lit(0.5) / 3) / f.log(f.lit(2)))
sy_by_sm = f.lit(2)+f.lit(f.log(f.lit(f.col("sm")) / 3) / f.log(f.lit(2)))


# In[7]:


tmp_df = join_df     .withColumn("end_date",                        f.when(replace_condition, join_df["update_date"])                        .otherwise(join_df["end_date"])                       )    .withColumn("end_y", f.year(f.col("end_date")))    .withColumn("end_m", f.month(f.col("end_date")))     .withColumn("start_y", f.year(f.col("start_date")))    .withColumn("start_m",f.month(f.col("start_date")))     .withColumn("sm",                 f.when(sm>750, f.lit(750))                 .otherwise(sm)
               ) \
    .withColumn("sy", f.when(f.col("sm")==0,sy_default)\
                       .otherwise(sy_by_sm)
               )
first_transform_df = tmp_df     .withColumn("diff_time", f.abs(middle_date_diff+update_date_diff))     .orderBy(["pkey","diff_time"], ascending = True)     .coalesce(1)     .dropDuplicates(["pkey", "diff_time"]).explain()

#     .drop("pkey","finish_date","start_date","end_date","update_date","idno","end_y","end_m","start_y","start_m","diff_time")


# In[16]:


# order by  cid select where cid[0]
# JOIN cid ON invoice
# fill cid = 0 when invoice == 0 or invoice is null

cid_df_select = cid_df .select("cid","invoice") .orderBy(["invoice","cid"], ascending = True) .coalesce(1) .dropDuplicates(["invoice"])

results = first_transform_df .join(cid_df_select,["invoice"],"left").nafill({'cid': 0}) .withColumn("cid", f.when((f.col("invoice")==0 | f.col("cid")==0) , f.lit(0))                    .otherwise(f.col("cid"))) 

# write output
results.write.format("parquet").mode("overwrite").save("s3a://aws-test-benny/sharon_inside/result.parquet")

#execution time
execution_time = datetime.datetime.now() - start_time


# In[17]:


print(f"Spend time: {execution_time}")


# In[18]:


results.count()


# In[6]:


results.printSchema()


# In[ ]:




