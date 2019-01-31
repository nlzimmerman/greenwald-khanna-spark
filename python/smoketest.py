#!/usr/bin/env python3

from com.github.nlzimmerman.GK import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# needed for python gk stuff
from math import floor, ceil
from pyspark.sql.types import BooleanType, StructField, StructType, StringType, IntegerType, TimestampType, DoubleType, LongType, ArrayType, MapType
from operator import itemgetter, add
from copy import copy
import numpy as np
import json

# TODO: move this to a library
def new_gk_dict(epsilon):
    return {
        # every item in sample will be a list of form
        # [v, g, delta]
        # I'm using lists and not tuples because I want to mutate
        # in-place to increase speed.
        "sample": list(),
        "epsilon": epsilon,
        "count": 0,
        "schema": "g-delta"
    }

def insert_gk(d, v):
    # this is a reference; that's intentional
    sample = d['sample']
    # remember that since count is an int,
    # an operation like count = d['count']
    # would create a "copy", not a reference
    #
    # this never changes
    epsilon = d['epsilon']
    # beware, sample_length is not the same thing as count at all
    # I just use it for convenience.
    sample_length = len(sample)
    # initially, all samples have g = 1
    # the beginning and end samples initially have delta = 0
    # and all the other samples have delta = floor(2*epsilon*count)
    # where count is the count at the time that sample was inserted
    if sample_length == 0 or v < sample[0][0]:
        sample.insert(0, [v,1,0])
    else:
        # scanning to see where v goes
        # we use this to catch the edge case where v is larger than any of the samples
        added = False
        # using an index instead of a more normal for loop so that we have an index to pass to list.insert
        # we're starting at 1 because we already checked element 0 up above
        for i in range(1,sample_length):
            if v < sample[i][0]:
                delta = floor(2*epsilon*d['count'])
                sample.insert(i, [v, 1, delta])
                added = True
                break
        # if v was never < sample[i][0]
        # then it's larger than all of them. In this case delta is 0 again
        if not added:
            sample.insert(sample_length, [v, 1, 0])
    # we've now added one element somewhere.
    # increment the count, see if it's time to compress, and return the dict
    d['count'] += 1
    if d['count'] % floor(1.0/(2.0*epsilon)) == 0:
        return compress_gk(d)
    else:
        return d

def compress_gk(d):
    # scan through the samples,
    # greedily looking to see if a value can be combined with its neighbor
    i = 1
    # using a while loop because len(d['sample']) changes
    while i < len(d['sample']) - 1:
        this_element = d['sample'][i]
        next_element = d['sample'][i+1]
        # if this.g + next.g + next.delta < …
        if (this_element[1]+next_element[1]+next_element[2]) < floor(2*d['epsilon']*d['count']):
            # next.g += this.g
            next_element[1] += this_element[1]
            d['sample'].pop(i)
            # we aren't incrementing i here because instead we removed the ith element
            # so the ith element is now next_sample and the i+1th element is the one after that
            # I am not _entirely_ sure that this is right. Other implementations incremented i
            # anyway. If I understand the math right. That's not going to produce a wrong answer
            # but it will produce an under-compressed sample.
        else:
            i += 1
    return d

def query_gk(d, quantile):
    desired_rank = ceil(quantile*(d['count']-1))
    rank_epsilon = d['epsilon']*d['count']
    starting_rank = 0
    for sample in d['sample']:
        starting_rank += sample[1]
        ending_rank = starting_rank + sample[2]
        if (
            ((desired_rank-starting_rank) <= rank_epsilon) and
            ((ending_rank-desired_rank) <= rank_epsilon)
        ):
            return sample[0]
    # if we incremented through all of the samples without finding one, that means
    # we want the last one
    return d['sample'][-1][0]

def combine_gk(g0, g1):
    # I tried to optimize this for speed which means there are some shortcuts I might not otherwise take.
    if len(g1['sample']) == 0:
        return g0
    elif len(g0['sample']) == 0:
        return g1
    # this is an online operation but these schemas are supposed to be small
    # the values in rank schemas are already sorted
    # so no need to re-sort
    # copy is a shallow copy so this should be cheap.
    # it also means that you need to not modify the contents without also copying those :)
    sample0 = copy(g0['sample'])
    sample1 = copy(g1['sample'])

    out = list()
    while len(sample0) > 0 and len(sample1) > 0:

        # take whichever lead item is smallest
        if sample0[0][0] < sample1[0][0]:
            # removes the tuple from sample0
            # the copy is so that changes here don't propagate back to g0
            # which can happen because sample is a shallow copy!
            # which can cause Spark weirdness.
            # g0 and g1 are both disposible here but I seem to recall that things get weird when you
            # mutate things you aren't supposed to mutate
            this_element = copy(sample0.pop(0))
            # None, 0, 0 is the default value here because if there is no larger element, we don't add
            # anything to delta.
            other_next_element = next((x for x in sample1 if x[0] > this_element[0]), (None, 0, 0))
        else:
            this_element = copy(sample1.pop(0))
            other_next_element = next((x for x in sample0 if x[0] > this_element[0]), (None, 0, 0))
        # v and g stay the same in this element
        # and delta becames
        # this_delta + other_next_delta + other_next_g -1
        new_delta = this_element[2] + other_next_element[2] + other_next_element[1] -1
        this_element[2] = new_delta
        out.append(this_element)
    # one list or the other is exhausted, so now we run whichever one isn't exhausted out
    # only one of these while loops will execute at all
    while len(sample0) > 0:
        this_element = copy(sample0.pop(0))
        out.append(this_element)
    while len(sample1) > 0:
        this_element = copy(sample1.pop(0))
        out.append(this_element)
    new_epsilon = max(g0['epsilon'], g1['epsilon'])
    count_increase = min(g0['count'],g1['count'])

    to_return = new_gk_dict(new_epsilon)
    to_return['sample'] = out
    to_return['count'] = g0['count'] + g1['count']
    # if it grew by enough to trigger a re-compression, do that
    # this should happen so often that maybe it isn't worth checking
    if count_increase >= floor(1/(2*new_epsilon)):
        return compress_gk(to_return)
    else:
        return to_return

def get_quantiles(df, group_by, value_column, quantiles=[0.5], epsilon=0.01, out_column_names=None):
  group_by_schema = [
    next((x for x in df.schema if x.name==y))
    for y in group_by
  ]
  if out_column_names is not None:
    assert len(out_column_names) == len(quantiles)
  else:
    out_column_names = [
      "{}_{:02d}".format(value_column, floor(q*100))
      for q in quantiles
    ]
  quantile_schema = [
    StructField(x, DoubleType(), False)
    for x in out_column_names
  ]
  final_schema = StructType(group_by_schema + quantile_schema)
  keyer = itemgetter(*group_by)

  df_with_quantiles = df.select(
    *([col(x) for x in group_by]+[col(value_column)])
  ).rdd.map(
    lambda x: (keyer(x), x[value_column])
  ).aggregateByKey(
    new_gk_dict(epsilon),
    insert_gk,
    combine_gk,
  ).mapValues(
    lambda x: [query_gk(x, y) for y in quantiles]
  ).map(
    lambda x: (
      *x[0],
      *x[1]
    )
  ).toDF(final_schema)

  return df_with_quantiles


if __name__ == "__main__":
    # read in the data
    data = list()
    with open("../data/zero.txt", "r") as f:
        for line in f.readlines():
            data.append(float(line))
            #print(float(line))
    print(np.quantile(data, [0.04, 0.05, 0.06]))
    # https://stackoverflow.com/questions/25193488/how-to-turn-off-info-logging-in-spark
    # print("Hello World!")
    # sparkSession = (
    #             SparkSession.
    #             builder.
    #             master("local[12]").
    #             appName("unit test").
    #             config("spark.jars", "../target/scala-2.11/greenwald-khanna-udaf_2.11-0.0.1.jar").
    #             getOrCreate()
    #         )
    # logger = sparkSession.sparkContext._jvm.org.apache.log4j
    # logger.LogManager.getLogger("org").setLevel( logger.Level.ERROR )
    # logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    # a = sparkSession.read.json("../data/debug0.json").cache()
    # gk = GKQuantile.gk_agg(sparkSession.sparkContext, [0.05, 0.5, 0.95], 0.01)
    # # time_df = a.groupBy(
    # #     ["product_id", "chapter_id", "knowledge_component_id", "probe_id"]
    # # ).agg(gk(col("time_to_answer_inferred")))
    # # print(time_df.collect())
    # # ugh, is there a better way to do this?
    # df = a.filter(
    #     (
    #         (col("product_id") == "145037") &
    #         (col("chapter_id") == "Chapter 5. Receivables and Sales") &
    #         (col("knowledge_component_id") == "CLe5632315-4008-4e4a-9285-0f7b3bddd976") &
    #         (col("probe_id") == "CL3fb73a28-9422-ef32-d642-12833bf95725")
    #     )
    # )
    # time_quantiles = get_quantiles(
    #     df,
    #     ["product_id", "chapter_id", "knowledge_component_id", "probe_id"],
    #     "time_to_answer_inferred",
    #     [0.05, 0.5, 0.95]
    # )
    # print(time_quantiles.collect())
    # local_data = df.rdd.map(lambda x: x["time_to_answer_inferred"]).collect()
    # print(np.quantile(local_data, [0.04, 0.05, 0.06]))
    # print(np.quantile(local_data, [0.49, 0.5, 0.51]))
    # print(np.quantile(local_data, [0.94, 0.95, 0.96]))
    # # g = GKQuantile(sparkSession)
    # # b = g.getGroupedQuantilesSQL(
    # #     a,
    # #     "name",
    # #     "value"
    # # )
    # # b.show()
    # # print(b.collect())
    # # print(b.rdd.map(lambda x: (x["name"], x["quantiles"])).collectAsMap())
    # # quantilizer = GKQuantile.gk_agg(sparkSession.sparkContext, [0.5], 0.01)
    # # print(a.groupBy().agg(quantilizer(col("value")).alias("q")).collect())
