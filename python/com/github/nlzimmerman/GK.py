from pyspark.sql import SparkSession
from pyspark.mllib.common import _java2py, _py2java

# As in the Scala, this class is expected to be a singleton, so all methods
# should be classmethods or staticmethods
class GKQuantile(object):

    @classmethod
    def spark(cls):
        return (
                SparkSession.
                builder.
                master("local[4]").
                appName("example").
                getOrCreate()
            )

    @classmethod
    def py2java(cls, x):
        return _py2java(cls.spark(), x)

    @classmethod
    def getQuantiles(
        cls,
        x, # RDD of some number
        quantiles = [0.5], # list of floats
        epsilon = 0.01
    ):
        pass

    @classmethod
    def scalaAdd(cls, x, y):
        s = cls.spark.sparkContext._jvm.com.github.nlzimmerman.Python.add
        return s(cls.py2java(x), cls.py2java(y))



if __name__ == "__main__":
    # https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-spark
    logger = GKQuantile.spark().sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    a = GKQuantile.spark().sparkContext.parallelize([1.0,2.0,3.0,4.0,5.0])
