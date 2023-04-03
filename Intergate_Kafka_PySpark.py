#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init() 


# In[2]:


from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import col


# In[3]:


scala_version = '2.12'  # TODO: Ensure this is correct
spark_version = '3.0.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.1'
]


# In[4]:


spark = SparkSession.builder   .master("local")   .appName("kafka-example")   .config("spark.jars.packages", ",".join(packages))   .getOrCreate()


# In[5]:


df = spark.read.csv('C:\\Users\\vnhongson\\Downloads\\National_Stock_Exchange_of_India_Ltd.csv',header=True)


# In[6]:


df.printSchema()


# In[7]:


df.selectExpr("CAST(Symbol AS STRING) AS key", "to_json(struct(*)) AS value")     .write.format("kafka").option("kafka.bootstrap.servers", "192.168.56.1:9092").option("topic", "test1").save()


# In[8]:


df_read = spark   .read   .format("kafka")   .option("kafka.bootstrap.servers", "192.168.56.1:9092")   .option("subscribe", "test1")   .load()


# In[9]:


df_read.show()


# In[14]:


final_data = df_read.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


# In[15]:


final_data 


# In[11]:


result = final_data.select('value').filter(final_data.key.isNotNull())


# In[12]:


result.show()


# In[16]:


df.printSchema()


# In[17]:


columns = StructType([StructField('Symbol',
                                      StringType(), True),
                          StructField('Open',
                                      StringType(), True),
                          StructField('High',
                                      StringType(), True),
                          StructField('Low',
                                      StringType(), True),
                          StructField('LTP',
                                      StringType(), True),
                          StructField('Chng',
                                      StringType(), True),
                          StructField('% Chng',
                                      StringType(), True),
                          StructField('Volume (lacs)',
                                      StringType(), True),
                          StructField('Turnover (crs.)',
                                      StringType(), True),
                          StructField('52w H',
                                      StringType(), True),
                          StructField('52w L',
                                      StringType(), True),
                          StructField('365 d % chng',
                                      StringType(), True),
                          StructField('30 d % chng',
                                      StringType(), True)])


# In[18]:


df_read_final = result.withColumn('c1', F.from_json('value', schema = columns)).select('c1.*')


# In[20]:


df_read_final.show()


# In[21]:


df_read_final.printSchema()


# In[ ]:




