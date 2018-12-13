from random import Random
from math import floor, ceil, log, cos, pi

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
                # WHOA
                # http://www.mathcs.emory.edu/~cheung/Courses/584-StreamDB/Syllabus/08-Quantile/Greenwald.html
                # https://www.stevenengelhardt.com/2018/03/07/calculating-percentiles-on-streaming-data-part-2-notes-on-implementing-greenwald-khanna/#GK01
                if d['count'] < 1.0/(2.0*epsilon):
                    delta = 0
                else:
                    delta = sample[i][1] + sample[i][2] -1
                sample.insert(i, [v, 1, delta])
                added = True
                break
        # if v was never < sample[i][0]
        # then it's larger than all of them. In this case delta is 0 again
        if not added:
            sample.insert(sample_length, [v, 1, 0])
    print(sample)
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


if __name__ == "__main__":
    d = new_gk_dict(0.1)
    for n in [ 11,20,18,5,12,6,3,2]:
        insert_gk(d, n)
    print(query_gk(d, 0.5))
    #print(d['sample'])
    # r = Random()
    # r.seed(2210)
    # numbers = list()
    # n = 100
    # for i in range(n):
    #     numbers.extend(
    #         [
    #             10*i+x for x in
    #                 [1,5,2,6,3,7,4,8,0,9]
    #         ]
    #     )
    # numbers.append(n*10)
    #
    # import numpy as np
    # print(np.quantile(numbers, [0.05, 0.5, 0.95]))
    # print(np.quantile(numbers, [0.04, 0.49, 0.94]))
    # d = new_gk_dict(0.01)
    # for n in numbers:
    #     insert_gk(d, n)
    # print([query_gk(d, x) for x in [0.05, 0.5, 0.95]])
    # print(d['sample'])
    # print(len(d['sample']))
