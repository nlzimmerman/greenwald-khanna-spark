from pyspark.sql import SparkSession

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
    def getQuantiles(
        cls,
        x, # RDD of some number
        quantiles = [0.5], # list of floats
        epsilon = 0.01
    ):
        pass



if __name__ == "__main__":
    # https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-spark
    logger = GKQuantile.spark().sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    a = GKQuantile.spark().sparkContext.parallelize([1,2,3,4,5])
    
