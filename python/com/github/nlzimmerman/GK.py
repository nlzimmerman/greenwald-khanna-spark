from pyspark.sql import SparkSession
from pyspark.ml.common import _java2py, _py2java


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
    def getGroupedQuantiles(
        self,
        x, #RDD of key-value pairs, where the value is a number of some type.
        quantiles, # list of numbers between 0 and 1,
        epsilon=0.01,
        force_type = float
    ):
        '''
        So, this wound up being a huge pain because I didn't know what I was doing.
        Here's the big idea: py4j works on Arrays of primitive types. So it's necessary
        to unpack those arrays into tuples before you do any Scala on them, and
        it's necessary to turn tuples back into arrays before pulling it back into the python.

        I haven't really dug into how pyspark.ml.common.{_py2java, _java2py}
        really work.
        '''
        # self.py2java turns our RDD of two-item tuples into an RDD[Array[Any]]
        # This function will cast that into an RDD[(String, Double)]
        t2 = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._StringDoubleToTuple2
        # This function will cast our RDD[((String, Double), Double)] into an RDD[Array[Any]]
        unpack = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._groupedQuantilesToPython
        if force_type is None:
            inferred_type = type(x.first()[1])
            if inferred_type is float:
                ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getGroupedQuantilesDouble
            elif inferred_type is int:
                ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getGroupedQuantilesInt
            else:
                raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
            x = t2(self.py2java(x))
        elif force_type is int:
            ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getGroupedQuantilesInt
            #x = self.py2java(x.mapValues(lambda x: int(x)))
            x = t2(self.py2java(x.mapValues(lambda x: int(x))))
        elif force_type is float:
            ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getGroupedQuantilesDouble
            x = t2(self.py2java(x.mapValues(lambda x: float(x))))
            #x = x.mapValues(lambda x: float(x))
        else:
            raise Exception("We can either force every value to be a float, an int, or do basic type inspection.")
        
        eps = self.py2java(epsilon)
        q = self.py2java(quantiles)
        out = ggq(x, q, eps)

        return self.java2py(unpack(out))
