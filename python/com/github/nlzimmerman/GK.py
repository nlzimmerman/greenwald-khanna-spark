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
                config("spark.jars", "../target/scala-2.11/greenwald-khanna-udaf_2.11-0.0.1.jar").
                getOrCreate()
            )

    @classmethod
    def java2py(cls, x):
        return _java2py(cls.spark(), x)

    @classmethod
    def py2java(cls, x):
        return _py2java(cls.spark(), x)

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
