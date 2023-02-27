import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import re

spark=SparkSession.builder.getOrCreate()
name='asl.csv'
ext=name.split('.')[-1]
file_name=name.split('.')[0]
bucket_name='gbatch-training-2023'


if ext == 'xls':
    df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("s3a://{}/{}/{}/{}/{}".format(bucket_name,'shubham','use_case_1','Inbound',name))
    df.toPandas().to_csv('newfile', index=False, sep = "|")
    res = pd.DataFrame(pd.read_csv("newfile"))
    res1=spark.createDataFrame(res)
    
elif ext=='csv':
    res1=spark.read.format('csv').option('header','true').option('inferSchema','true').load("s3a://{}/{}/{}/{}/{}".format(bucket_name,'shubham','use_case_1','Inbound',name))
    
#transformation
cols = [re.sub("[^0-9a-zA-Z ]", "", col) for col in res1.columns]
res2 = res1.toDF(*cols)

res3=res2.withColumn('Date',to_date('Date','M/d/yyyy')).withColumn("Week_num", concat(lit("Week"), weekofyear('Date')).cast(StringType()))\
    .withColumn('year',year("Date")).withColumnRenamed('Scrip name','Scrip_name').withColumnRenamed('Adj Close','Adj_Close').dropna()

res4 = res3.groupBy('Week_num','Scrip_name').agg(sum('Volume').alias('Volume'),
                                             min('Date').alias('St_date'),
                                             max('Date').alias('End_Date'),
                                             first('Open').alias('Open'),
                                             max('High').alias('High'),
                                             min('Low').alias('Low'),
                                             last('Close').alias('Close'),
                                             avg('Adj_Close').alias('Adj_Close'))

res5 = res4.select('Scrip_name','Week_num','St_date','End_Date','Open','High','Low','Close','Adj_Close','Volume')

#sending file into the landing bucket   
#res1.coalesce(1).write.format('csv').option('header','true').save("s3a://{}/{}/{}/{}/{}".format(bucket_name,'shubham','use_case_1','Landing',file_name)) 


#converting csv format to parquet format

#res1.coalesce(1).write.format('csv').option('header','true').save("s3a://{}/{}/{}".format(bucket_name,'Standardized',file_name))
res1.coalesce(1).write.option('compression','none').format('parquet').option('header','true').save("s3a://{}/{}/{}/{}/{}".format(bucket_name,'shubham','use_case_1','Standardized',file_name))



