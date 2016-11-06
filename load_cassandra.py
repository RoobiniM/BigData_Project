# -*- coding: utf-8 -*-
from pyspark.sql import SQLContext
from pyspark import SparkConf
import sys
import pyspark_cassandra

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        keyspace = sys.argv[2]
        table = sys.argv[3]
    
    # initialize spark cassandra     
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds))
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    #Process input file based on table_name 
    if table == "yelp_review":
        df = sqlContext.read.format('com.databricks.spark.csv') \
            .options(header='true').load(inputs) \
            .select("user_id","review_id","text","votes_cool","business_id","votes_funny", \
            "stars","date","votes_useful")
        rdd = df.rdd.map(lambda line: (line[0],line[1],line[2],int(line[3]), \
            line[4],int(line[5]),int(line[6]),line[7], int(line[8])))
        columns = ["user_id","review_id","text","votes_cool","business_id","votes_funny", \
            "stars","date","votes_useful"]
            
    elif table == "yelp_business":
        df = sqlContext.read.format('com.databricks.spark.csv') \
            .options(header='true').load(inputs) \
            .select("business_id","name","review_count","state","full_address",\
            "open","city","latitude","longitude","stars")
        rdd = df.rdd.map(lambda line: (line[0],line[1],int(line[2]),line[3], \
            line[4],line[5],line[6],float(line[7]),float(line[8]),float(line[9])))
        columns = ["business_id","name","review_count","state","full_address",\
            "open","city","latitude","longitude","stars"]
    
    #Save result to Cassandra
    rdd.saveToCassandra(keyspace, table, columns=columns, batch_size=300, parallelism_level=1000 )          
        
if __name__ == "__main__":
    main()