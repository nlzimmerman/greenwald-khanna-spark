from pyspark.sql import SparkSession
from pyspark.mllib.common import _java2py, _py2java
import json

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
        return _java2py(self.spark().sparkContext, x)

    def py2java(self, x):
        return _py2java(self.spark().sparkContext, x)

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
        x, #RDD of key-value pairs, where
           # the key MUST be a string
           # the value can be an int or a float
        quantiles, # list of numbers between 0 and 1,
        epsilon=0.01,
        force_type = float,
    ):
        '''
        So, this wound up being a huge pain because I didn't know what I was doing.
        Here's the big idea: py4j treats python tuples the same way it treats python lists,
        so it turns them into Arrays of primitive types. Since my Scala code works on Tuple2s,
        it's necessary to unpack those arrays into tuples before you do any Scala on them, and
        it's necessary to turn tuples back into arrays before pulling it back into the python.

        If encode_keys is False, the keys MUST be a string.
        '''
        # there may be a problem with this; it fails a unit test when encode_keys is True
        # taking it out for now. Beware!
        # encode_keys = False
        # if encode_keys is True:
        #     key_map = x.keys().distinct().map(lambda y: (y, json.dumps(y)))
        #     reverse_key_map = key_map.map(lambda y: (y[1], y[0]))
        #     # swap out the old key with the new one.
        #     x = x.join(key_map).map(
        #         lambda y: (y[1][1], y[1][0])
        #     )

        if force_type is None:
            inferred_type = type(x.first()[1])
            if inferred_type is float:
                ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesDouble
            elif inferred_type is int:
                ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesInt
            else:
                raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
            x = self.py2java(x)
        elif force_type is int:
            ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesInt
            #x = self.py2java(x.mapValues(lambda x: int(x)))
            x = self.py2java(x.mapValues(lambda x: int(x)))
        elif force_type is float:
            ggq = self.spark().sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesDouble
            x = self.py2java(x.mapValues(lambda x: float(x)))
            #x = x.mapValues(lambda x: float(x))
        else:
            raise Exception("We can either force every value to be a float, an int, or do basic type inspection.")

        eps = self.py2java(epsilon)
        q = self.py2java(quantiles)
        out = self.java2py(ggq(x, q, eps))
        # if we encoded the keys, now we have to get them back.
        # if encode_keys is True:
        #     out = out.map(lambda x: (x[0][0], (x[0][1], x[1]))).join(reverse_key_map).map(
        #         lambda x: ((x[1][1], x[1][0][0]), x[1][0][1])
        #     )
        return out
