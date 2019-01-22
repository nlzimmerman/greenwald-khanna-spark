from pyspark.sql import SparkSession
from pyspark.mllib.common import _java2py, _py2java
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col

import json

# no longer a singleton because we have some unavoidable install-specific configuration

class GKQuantile(object):

    def __init__(
        self,
        sparkSession
    ):
        self.sparkSession = sparkSession


    # def spark(self):
    #     return (
    #             SparkSession.
    #             builder.
    #             master(self.master).
    #             appName(self.app_name).
    #             config("spark.jars", self.jar_path).
    #             getOrCreate()
    #         )

    def java2py(self, x):
        return _java2py(self.sparkSession.sparkContext, x)

    def py2java(self, x):
        return _py2java(self.sparkSession.sparkContext, x)

    # just for debugging
    # do remove this at some point
    def scalaAdd(self, x, y):
        s = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.Python.add
        return s(self.py2java(x), self.py2java(y))

    # this method is for the use case where you have a spark SQL workflow
    # in place, you use df.groupBy().agg() all the time, and you aren't
    # interested in instantiating this class.
    # This is the way I would tend to do it at work.
    # This function returns a function that can be used with .agg()
    @staticmethod
    def gk_agg(
        sc, # this is a SPARK CONTEXT, not a Spark Session,.
            # we have to have the spark context so we can find the function
            # on the JVM
        quantiles, # quantiles to find,
        epsilon, # precision
    ):
        q = _py2java(sc, quantiles)
        e = _py2java(sc, epsilon)
        def _gk(col):
            UntypedGKAggregator_instance = (
                sc._jvm.com.github.nlzimmerman.UntypedGKAggregator(q, e)
            )
            instance_applier = UntypedGKAggregator_instance.apply
            return Column(
                instance_applier(
                    _to_seq(
                        sc,
                        [col],
                        _to_java_column
                    )
                )
            )
        return _gk

    def getGroupedQuantilesSQL(
        self,
        df, # DF with some key column that responds to GroupBy
           # and some value column that is numeric.
        groupByColumn, # string containing the column to key by
        aggregationColumn, # string containing the column to aggregate over.
        outputColumnName = "quantiles",
        quantiles = [0.5], # Python list containing the quantiles to compute
        epsilon = 0.01
    ):  # no force_type here because the Scala casts everything to Double whether you like it or not.
        eps = self.py2java(epsilon)
        q = self.py2java(quantiles)
        # cribbed from
        # http://www.cyanny.com/2017/09/15/spark-use-scala-udf-udaf-in-pyspark/
        def gk(col):
            # to cut down on verbiage
            sc = self.sparkSession.sparkContext
            # oof, let's see if this works
            # update — it seems to! I was kind of guessing
            ugk_instance = (
                sc._jvm.com.github.nlzimmerman.UntypedGKAggregator(q, eps)
            )
            _untypedGKAggregator = ugk_instance.apply
            return Column(
                _untypedGKAggregator(
                    _to_seq(
                        sc,
                        [col],
                        _to_java_column
                    )
                )
            )
        return df.groupBy(col(groupByColumn)).agg(gk(col(aggregationColumn)).alias(outputColumnName))



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
                gq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesDouble
            elif inferred_type is int:
                gq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesInt
            else:
                raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
            x = self.py2java(x)
        elif force_type is int:
            gq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesInt
            x = self.py2java(x.map(lambda y: int(y)))
        elif force_type is float: # force_type is float, beccause we've already
            gq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._getQuantilesDouble
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
                ggq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesDouble
            elif inferred_type is int:
                ggq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesInt
            else:
                raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
            x = self.py2java(x)
        elif force_type is int:
            ggq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesInt
            #x = self.py2java(x.mapValues(lambda x: int(x)))
            x = self.py2java(x.mapValues(lambda x: int(x)))
        elif force_type is float:
            ggq = self.sparkSession.sparkContext._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesDouble
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
