import unittest
from com.github.nlzimmerman.GK import *


class GKTest(unittest.TestCase):
    a = GKQuantile.spark().sparkContext.parallelize([1.0,2.0,3.0,4.0,5.0])
    b = GKQuantile.spark().sparkContext.parallelize([10,20,30,40,50])
    def test_sanity(self):
        self.assertEqual(self.a.count(), 5)
    def test_quantile(self):
        x = GKQuantile.getQuantiles(self.a, [0.5], force_type = None)
        # self.assertEqual(len(x), 1)
        # self.assertEqual(type(x[0]), float)
        # self.assertEqual(x[0], 3.0)
