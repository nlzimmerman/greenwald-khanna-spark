import unittest
from com.github.nlzimmerman.GK import *
from scipy.special import erf, erfinv
from math import log, sin, cos, pi, pow
import random

class BasicTest(unittest.TestCase):
    def setUp(self):
        self.g = GKQuantile()
        self.a = self.g.spark().sparkContext.parallelize([1.0,2.0,3.0,4.0,5.0])
        self.b = self.g.spark().sparkContext.parallelize([10,20,30,40,50])
        self.normal = NormalNumbers()
        # these are NOT the same targets I used in the Scala tests, just to mix it up
        self.targets = [
            0.05,
            0.22,
            0.5,
            0.82,
            0.95
        ]
    @unittest.skip("")
    def test_sanity(self):
        self.assertEqual(self.a.count(), 5)
    @unittest.skip("")
    def test_quantile_float_simple(self):
        x = self.g.getQuantiles(self.a, [0.5], force_type = None)
        self.assertEqual(len(x), 1)
        self.assertEqual(type(x[0]), float)
        self.assertEqual(x[0], 3.0)
    @unittest.skip("")
    def test_quantile_int_simple(self):
        x = self.g.getQuantiles(self.b, [0.5], force_type = None)
        self.assertEqual(len(x), 1)
        self.assertEqual(type(x[0]), int)
        self.assertEqual(x[0], 30)
    # duplicate the tests that happen in the scala as closely as we can.
    # we're only including the spark tests since those are all that have
    # python wrappers.
    @unittest.skip("")
    def test_normal_distribution_spark(self):
        n0 = self.g.spark().sparkContext.parallelize(self.normal.numbers, 100)
        for epsilon in [0.005, 0.01, 0.05]:
            bounds = Util.inverseNormalCDFBounds(self.targets, epsilon)
            # getQuantiles returns a LIST, not an RDD
            n = self.g.getQuantiles(n0, self.targets, epsilon)
            self.assertEqual(len(bounds), len(n))
            for b, x in zip(bounds, n):
                self.assertTrue(b[0] <= x)
                self.assertTrue(x <= b[1])
    def test_normal_groupBy_spark(self):
        ''' inversion by key '''
        n0 = self.g.spark().sparkContext.parallelize(self.normal.numbers, 100).map(lambda x: ("a", x))
        n1 = self.g.spark().sparkContext.parallelize(self.normal.numbers2, 100).map(lambda x: ("b", x))
        nr = n0.union(n1).repartition(100)
        for epsilon in [0.005]:
            bounds = Util.inverseNormalCDFBounds(self.targets, epsilon)
            quantiles = self.g.getGroupedQuantiles(nr, self.targets, epsilon, force_type = None)
            print(quantiles.collectAsMap())
            #print("Q")
            #print(nr.ctx)
            #print(quantiles.count())





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
