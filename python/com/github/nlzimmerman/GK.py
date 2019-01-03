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
    def java2py(cls, x):
        return _java2py(cls.spark(), x)

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

    # just for debugging
    @classmethod
    def scalaAdd(cls, x, y):
        s = cls.spark().sparkContext._jvm.com.github.nlzimmerman.Python.add
        return s(cls.py2java(x), cls.py2java(y))

    @classmethod
    def getQuantiles(
        cls,
        x, #RDD of some numeric type
        quantiles, #list of numbers between 0 and 1
        epsilon = 0.01,
        force_type = float
    ):
        # force_type can be float, int, or None
        #if force_type not in {float, int, None}:

        if force_type is None:
            inferred_type = type(x.first())
            if inferred_type is float:
                gq = cls.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesDouble
            elif inferred_type is int:
                gq = cls.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesInt
            else:
                raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
            x = cls.py2java(x)
        elif force_type is int:
            gq = cls.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesInt
            x = cls.py2java(x.map(lambda y: int(y)))
        elif force_type is float: # force_type is float, beccause we've already
            gq = cls.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesDouble
            x = cls.py2java(x.map(lambda y: float(y)))
        else:
            raise Exception("We can either force everything to be a float, an int, or do basic type inspection.")
        eps = cls.py2java(epsilon)
        q = cls.py2java(quantiles)
        return cls.java2py(gq(x, q, eps))



if __name__ == "__main__":
    # https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-spark
    # print("Hello World!")
    logger = GKQuantile.spark().sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    a = GKQuantile.spark().sparkContext.parallelize([1.0,2.0,3.0,4.0,5.0])
    b = GKQuantile.spark().sparkContext.parallelize([10,20,30,40,50])
    print(a.reduce(lambda x, y: x+y))
    z = GKQuantile.scalaAdd(6,3.0)
    print(type(z))
    print(z)
    x = GKQuantile.getQuantiles(a, [0.5], force_type = None)
    print(type(x))
    print(x)
    y = GKQuantile.getQuantiles(b, [0.5], force_type = int)
    print(type(y))
    print(y)
