import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, struct, length

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

print("Job is starting...")

tedx_dataset_path = "s3://unibg-data-2021-davide/tedx_dataset.csv"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", "true") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY

count_items = tedx_dataset.count()
tedx_dataset = tedx_dataset.filter("idx is not null")
count_items_null = tedx_dataset.count()
tedx_dataset = tedx_dataset.filter("idx NOT LIKE '%[ ]%'")
count_correct_id = tedx_dataset.count()
tedx_dataset = tedx_dataset.filter(length(col('idx')) == 32)
count_idx_length = tedx_dataset.count()



print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")
print(f"Number of items from RAW DATA with NOT SPACES {count_correct_id}")
print(f"Number of items from RAW DATA with LENGTH {count_idx_length}")


# READ TAGS DATASET
tags_dataset_path = "s3://unibg-data-2021-davide/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

# ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
ted_tags = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \

ted_tags.printSchema()

# READ WATCH NEXT DATASET
watch_next_path  = "s3://unibg-data-2021-davide/watch_next_dataset.csv"
watch_next_dataset = spark.read.option("header","true").csv(watch_next_path)

watch_next_dataset.printSchema()

# FILTER OPERATIONS
print(f"Before deleting duplicates the rows are: {watch_next_dataset.count()}")
watch_next_dataset = watch_next_dataset.dropDuplicates()
print(f"After deleting duplicates the rows are: {watch_next_dataset.count()}")
watch_next_dataset = watch_next_dataset.filter('url LIKE "https://www.ted.com/talks/%"')
print(f"After deleting urls the rows are: {watch_next_dataset.count()}")

# ADD WATCH NEXT TO TEDX_DATASET
watch_next_dataset = watch_next_dataset.select(col("idx").alias("id_ref"), col("watch_next_idx"), col("url").alias("url_wn"))

watch_next_dataset = tedx_dataset.join(watch_next_dataset, tedx_dataset.idx == watch_next_dataset.watch_next_idx, "right") \
    .drop("idx") \
    .drop("url") \
    .drop("posted") \
    .drop("num_views") \
    
watch_next_dataset = watch_next_dataset.groupBy(col("id_ref")).agg(collect_list(struct("watch_next_idx", "url_wn", "main_speaker", "title", "details")).alias("watch_next_obj"))

ted_tags_wn = ted_tags.join(watch_next_dataset, ted_tags.idx == watch_next_dataset.id_ref, "left") \
    .drop("id_ref") \

ted_tags_wn.printSchema()
# TODO:FILTER OPERATIONS

# READ QUESTION DATASET
question_path  = "s3://unibg-data-2021-davide/question_ted_dataset.csv"
question_dataset = spark.read.option("header","true").csv(question_path)

question_dataset.printSchema()

# ADD QUESTION TO TED_X DATASET
question_dataset = question_dataset.groupBy(col("idx").alias("id_ref")).agg(collect_list(struct("question", "ans1","ans2","ans3","ans4","correct_ans","level")).alias("questions_obj"))

question_dataset.printSchema()

ted_tags_wn_question = ted_tags_wn.join(question_dataset, ted_tags_wn.idx == question_dataset.id_ref, "left") \
    .drop("id_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \
    

mongo_uri = "mongodb://mycluster-shard-00-00.gxjf7.mongodb.net:27017,mycluster-shard-00-01.gxjf7.mongodb.net:27017,mycluster-shard-00-02.gxjf7.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "db",
    "password": "dbuser",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(ted_tags_wn_question, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
