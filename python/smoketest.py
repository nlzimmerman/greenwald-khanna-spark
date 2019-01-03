from com.github.nlzimmerman.GK import *


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
