#!/usr/bin/env python3

from com.github.nlzimmerman.GK import *
from pyspark.sql import SparkSession


if __name__ == "__main__":
    # https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-spark
    # print("Hello World!")
    sparkSession = (
                SparkSession.
                builder.
                master("local[4]").
                appName("unit test").
                config("spark.jars", "../target/scala-2.11/greenwald-khanna-udaf_2.11-0.0.1.jar").
                getOrCreate()
            )
    logger = sparkSession.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    a = sparkSession.sparkContext.parallelize(
        [
            ("a",1),
            ("a",2),
            ("a",3),
            ("a",4),
            ("a",5),
            ("b",6),
            ("b",7),
            ("b",8),
            ("b",9),
            ("b",10),
        ]
    ).toDF(["name", "value"])
    a.show()
    g = GKQuantile(sparkSession)
    b = g.getGroupedQuantilesSQL(
        a,
        "name",
        "value"
    )
    b.show()
    print(b.collect())
    print(b.rdd.map(lambda x: (x["name"], x["quantiles"])).collectAsMap())
    quantilizer = GKQuantile.gk_agg(sparkSession.sparkContext, [0.5], 0.01)
    print(a.groupBy().agg(quantilizer(col("value")).alias("q")).collect())
