from pyspark.sql import SparkSession
from pyspark.mllib.common import _java2py, _py2java


# no longer a singleton because we have some unavoidable install-specific configuration

class GKQuantile(object):

    def __init__(
        self,
        master="local[4]",
        app_name="example",
        jar_path= "../target/scala-2.11/greenwald-khanna-udaf_2.11-0.0.1.jar"
    ):
        self.master = master
        self.app_name = app_name
        self.jar_path = jar_path

    def spark(self):
        return (
                SparkSession.
                builder.
                master(self.master).
                appName(self.app_name).
                config("spark.jars", self.jar_path).
                getOrCreate()
            )

    def java2py(self, x):
        return _java2py(self.spark(), x)

    def py2java(self, x):
        return _py2java(self.spark(), x)

    # just for debugging
    # do remove this at some point
    def scalaAdd(self, x, y):
        s = self.spark().sparkContext._jvm.com.github.nlzimmerman.Python.add
        return s(self.py2java(x), self.py2java(y))

    def getQuantiles(
        self,
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
                gq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesDouble
            elif inferred_type is int:
                gq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesInt
            else:
                raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
            x = self.py2java(x)
        elif force_type is int:
            gq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesInt
            x = self.py2java(x.map(lambda y: int(y)))
        elif force_type is float: # force_type is float, beccause we've already
            gq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesDouble
            x = self.py2java(x.map(lambda y: float(y)))
        else:
            raise Exception("We can either force everything to be a float, an int, or do basic type inspection.")
        eps = self.py2java(epsilon)
        q = self.py2java(quantiles)
        return self.java2py(gq(x, q, eps))
