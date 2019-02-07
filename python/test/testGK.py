import unittest
from com.github.nlzimmerman.GK import *
from scipy.special import erf, erfinv
from math import log, sin, cos, pi, pow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce
import random

class BasicTest(unittest.TestCase):
    def setUp(self):
        self.sparkSession = (
                    SparkSession.
                    builder.
                    master("local[4]").
                    appName("unit test").
                    config("spark.jars", "../target/scala-2.11/greenwald-khanna-udaf_2.11-0.0.1.jar").
                    getOrCreate()
                )
        self.a = self.sparkSession.sparkContext.parallelize([1.0,2.0,3.0,4.0,5.0])
        self.b = self.sparkSession.sparkContext.parallelize([10,20,30,40,50])
        self.normal = NormalNumbers()
        # these are NOT the same targets I used in the Scala tests, just to mix it up
        self.targets = [
            0.05,
            0.22,
            0.5,
            0.82,
            0.95
        ]
        n0 = self.sparkSession.sparkContext.parallelize(self.normal.numbers, 100).map(lambda x: ("a", x))
        n1 = self.sparkSession.sparkContext.parallelize(self.normal.numbers2, 100).map(lambda x: ("b", x))
        self.labeled_normal_numbers = n0.union(n1).toDF(["name", "value"]).repartition(100).cache()
    def test_sanity(self):
        self.assertEqual(self.a.count(), 5)
    def test_quantile_float_simple(self):
        x = getQuantiles(self.a, [0.5], force_type = None)
        self.assertEqual(len(x), 1)
        self.assertEqual(type(x[0]), float)
        self.assertEqual(x[0], 3.0)
    def test_quantile_int_simple(self):
        x = getQuantiles(self.b, [0.5], force_type = None)
        self.assertEqual(len(x), 1)
        self.assertEqual(type(x[0]), int)
        self.assertEqual(x[0], 30)
    # duplicate the tests that happen in the scala as closely as we can.
    # we're only including the spark tests since those are all that have
    # python wrappers.
    def test_normal_distribution_spark(self):
        n0 = self.sparkSession.sparkContext.parallelize(self.normal.numbers, 100)
        for epsilon in [0.005, 0.01, 0.05]:
            bounds = Util.inverseNormalCDFBounds(self.targets, epsilon)
            # getQuantiles returns a LIST, not an RDD
            n = getQuantiles(n0, self.targets, epsilon)
            self.assertEqual(len(bounds), len(n))
            for b, x in zip(bounds, n):
                self.assertTrue(b[0] <= x)
                self.assertTrue(x <= b[1])
    def test_normal_groupBy_spark(self):
        ''' inversion by key, NOT using Spark '''
        for epsilon in [0.005, 0.01, 0.05]:
            bounds = Util.inverseNormalCDFBounds(self.targets, epsilon)
            quantiles = getGroupedQuantiles(
                self.labeled_normal_numbers.rdd.map(lambda x: (x['name'], x['value'])),
                self.targets,
                epsilon,
                force_type = None
                )
            # I'm going to do the check in Spark instead of with a collect, just for fun.
            bounds_list = [
                (key, {t: b for t, b in zip(self.targets, bounds)})
                    for key in ("a", "b")
            ]
            bounds_rdd = self.sparkSession.sparkContext.parallelize(bounds_list)
            # this takes as input a tuple,
            # with item 0 as the bounds (as a dict (quantile -> (lower, upper)))
            # and item 1 as the value (quantile -> value)
            # it checks that the value is between lower and upper for all quantiles.
            # it's a generator so use it with a flatMap
            def checker(v):
                for quantile in v[0].keys():
                    lower_bound = v[0][quantile][0]
                    upper_bound = v[0][quantile][1]
                    value = v[1][quantile]
                    yield (
                        (lower_bound <= value) and
                        (value <= upper_bound)
                    )
            # this check is too clever since it won't tell you
            # which of the bounds fails. But I did do a more thorough check
            # in the Scala unit tests.
            self.assertTrue(
                bounds_rdd.join(
                    quantiles
                ).flatMapValues(checker).values().reduce(
                    lambda x, y: x and y
                )
            )
    def test_normal_groupBy_spark_sql(self):
        '''using the spark SQL aggregator'''
        for epsilon in [0.01]:
            bounds = {
                # quantile target: (lower bound, upper bound)
                t: b for t, b in zip(
                        self.targets,
                        Util.inverseNormalCDFBounds(self.targets, epsilon)
                    )
            }
            quantiles_df = getGroupedQuantilesSQL(
                self.labeled_normal_numbers,
                "name",
                "value",
                "quantiles",
                self.targets,
                epsilon
            )
            quantiles_dict = quantiles_df.rdd.map(
                lambda x: (x["name"], x["quantiles"])
            ).collectAsMap()
            for key in ["a", "b"]:
                for target in self.targets:
                    self.assertTrue(
                        bounds[target][0] <= quantiles_dict[key][target]
                    )
                    self.assertTrue(
                        quantiles_dict[key][target] <= bounds[target][1]
                    )
    def test_gk_agg(self):
        '''using gk_agg at epsilon=0.01'''
        #gk = self.g.gk_agg(self.sparkSession.sparkContext, self.targets, 0.01)
        gk = approximateQuantile(self.targets, 0.01)
        quantiles_df = self.labeled_normal_numbers.groupBy(col("name")).agg(gk(col("value")).alias("quantiles"))
        quantiles_dict = quantiles_df.rdd.map(
            lambda x: (x["name"], x["quantiles"])
        ).collectAsMap()
        bounds = {
            # quantile target: (lower bound, upper bound)
            t: b for t, b in zip(
                    self.targets,
                    Util.inverseNormalCDFBounds(self.targets, 0.01)
                )
        }
        for key in ["a", "b"]:
            for target in self.targets:
                self.assertTrue(
                    bounds[target][0] <= quantiles_dict[key][target]
                )
                self.assertTrue(
                    quantiles_dict[key][target] <= bounds[target][1]
                )
    # def test_approximateQuantile(self):
    #     '''using gk_agg at epsilon=0.01'''
    #     aq = self.g.approximateQuantile(self.targets, 0.01)
    #     quantiles_df = self.labeled_normal_numbers.groupBy(col("name")).agg(aq(col("value")).alias("quantiles"))
    #     quantiles_dict = quantiles_df.rdd.map(
    #         lambda x: (x["name"], x["quantiles"])
    #     ).collectAsMap()
    #     bounds = {
    #         # quantile target: (lower bound, upper bound)
    #         t: b for t, b in zip(
    #                 self.targets,
    #                 Util.inverseNormalCDFBounds(self.targets, 0.01)
    #             )
    #     }
    #     for key in ["a", "b"]:
    #         for target in self.targets:
    #             self.assertTrue(
    #                 bounds[target][0] <= quantiles_dict[key][target]
    #             )
    #             self.assertTrue(
    #                 quantiles_dict[key][target] <= bounds[target][1]
    #             )



    def test_int_groupBy_spark_simple(self):
        n0 = self.sparkSession.sparkContext.parallelize([0,1,2,3,4]).map(lambda x: ("foo", x))
        q = getGroupedQuantiles(n0, [0.5], 0.01, force_type = None)
        x = q.collectAsMap()["foo"][0.5]
        self.assertEqual(x, 2)
        self.assertTrue(type(x) is int)





class Util(object):

    @staticmethod
    def inverseNormalCDF(q):
        return (2**0.5)*erfinv(2*q-1)
    @staticmethod
    def normalCDF(x):
        return 0.5*(1+erf(x*(2**-0.5)))

    @staticmethod
    def inverseNormalCDFBounds(quantiles, epsilon):
        return [
            (
                Util.inverseNormalCDF(q-epsilon),
                Util.inverseNormalCDF(q+epsilon)
            )
                for q in quantiles
        ]

class NormalNumbers(object):
    def __init__(self, seed=2210):
        self.r = random.Random()
        self.r.seed(seed)
        # this is the EXACT normal distribution, to make sure that DirectQuantile works
        self.exact_numbers = [
            Util.inverseNormalCDF(x/500000) for x in range(1, 500000)
        ]
        self.r.shuffle(self.exact_numbers)
        self.numbers = [
            self.next_normal() for _ in range(500000)
        ]
        self.numbers2 = [
            self.next_normal() for _ in range(500000)
        ]
    def next_normal(self):
        u = self.r.random()
        v = self.r.random()
        return pow(-2 * log(u), 0.5) * cos(2*pi*v)


if __name__ == "__main__":
    unittest.main()
