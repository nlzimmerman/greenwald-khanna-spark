from pyspark.sql import SparkSession
from pyspark.mllib.common import _java2py, _py2java
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col
from pyspark import SparkContext


import json

# Formerly, this lived in a singleton class, but I see that pyspark.sql.functions
# puts its functions directly into the package. I'm emulating that pattern here.

def java2py(x):
    sc = SparkContext._active_spark_context
    return _java2py(sc, x)

def py2java(x):
    sc = SparkContext._active_spark_context
    return _py2java(sc, x)

# This is likely to be the entry-point into this package you want.
# Use it when you have a spark dataframe where you want to use
# df.groupBy().agg()
# for example, if you have a df(["name", "value"]),
# you would run
# df.groupBy(col("name")).agg(approximateQuantile([0.5], 0.05)(col("value")))
# to get the median.
def approximateQuantile(
    quantiles,    # quantiles to find, as a python list full of doubles 0 <= x <= 1
    epsilon=0.01, # precision, as a double. The default is likely to be good for you.
):
    sc = SparkContext._active_spark_context
    q = _py2java(sc, quantiles)
    e = _py2java(sc, epsilon)
    # this page is a great reference for this
    # http://www.cyanny.com/2017/09/15/spark-use-scala-udf-udaf-in-pyspark/
    # Our case is kind of interesting because the UDAF itself takes parameters.
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

# A convenience function that calls the above. I wrote these mostly to follow
# the pattern in the Scala, and I wrote the Scala functions the way I did just
# so that people who weren't using SQL wouldn't have to remember treeAggregate
# syntax.
def getGroupedQuantilesSQL(
    df,                # DF with some key column that responds to GroupBy
                       # and some value column that is numeric.
    groupByColumn,     # string containing the column to key by
    aggregationColumn, # string containing the column to aggregate over.
    outputColumnName = "quantiles", # string containing the
    quantiles = [0.5], # Python list containing the quantiles to compute
    epsilon = 0.01
):  # no force_type here because the Scala casts everything to Double whether you like it or not.
    gk = approximateQuantile(
        quantiles,
        epsilon
    )
    return df.groupBy(col(groupByColumn)).agg(gk(col(aggregationColumn)).alias(outputColumnName))


# Take calculate quantiles over an RDD of
# numbers, with no groupBy performed.
# It can attempt to infer and preserve types if you pass force_type=None, or
# it can force everything to be an int or a float.
# This returns a *list* where the nth element corresponds to the
# nth element of the quantiles argument.
# TODO: make the output a dict (this involved changing the scala).
def getQuantiles(
    x,         #RDD of some numeric type
    quantiles, #list of numbers between 0 and 1
    epsilon = 0.01,
    force_type = float
):
    # force_type can be float, int, or None
    #if force_type not in {float, int, None}:
    sc = SparkContext._active_spark_context
    if force_type is None:
        inferred_type = type(x.first())
        if inferred_type is float:
            gq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetQuantilesDouble
        elif inferred_type is int:
            gq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetQuantilesInt
        else:
            raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
        x = py2java(x)
    elif force_type is int:
        gq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetQuantilesInt
        x = py2java(x.map(lambda y: int(y)))
    elif force_type is float: # force_type is float, beccause we've already
        gq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetQuantilesDouble
        x = py2java(x.map(lambda y: float(y)))
    else:
        raise Exception("We can either force everything to be a float, an int, or do basic type inspection.")
    eps = py2java(epsilon)
    q = py2java(quantiles)
    return java2py(gq(x, q, eps))

# Analogous to above, but instead of working on an RDD of numbers, it
# works on an RDD of key/value pairs, where the key a string
# (other types may work, but I can't guarantee that e.g. tuples will work because
# of the way Python objects get passed into java.
# this returns an RDD
def getGroupedQuantiles(
    x, #RDD of key-value pairs, where
       # the key MUST be a string or something else that BOTH
       # + goes through _py2java without turning into anything weird AND
       # + is amenable to being a key for a key/value RDD.
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
    '''
    sc = SparkContext._active_spark_context
    if force_type is None:
        inferred_type = type(x.first()[1])
        if inferred_type is float:
            ggq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesDouble
        elif inferred_type is int:
            ggq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesInt
        else:
            raise Exception("couldn't figure out what to do with type {}".format(inferred_type))
        x = py2java(x)
    elif force_type is int:
        ggq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesInt
        #x = self.py2java(x.mapValues(lambda x: int(x)))
        x = py2java(x.mapValues(lambda x: int(x)))
    elif force_type is float:
        ggq = sc._jvm.com.github.nlzimmerman.GKQuantile._PyGetGroupedQuantilesDouble
        x = py2java(x.mapValues(lambda x: float(x)))
        #x = x.mapValues(lambda x: float(x))
    else:
        raise Exception("We can either force every value to be a float, an int, or do basic type inspection.")

    eps = py2java(epsilon)
    q = py2java(quantiles)
    out = java2py(ggq(x, q, eps))

    return out
