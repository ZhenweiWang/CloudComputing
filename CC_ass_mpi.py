#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mpi4py import MPI
import sys
import datetime
from collections import Counter
import re

comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()


# initialize the query word, users can input query word as well.
# default query word is Zhenwei.

query = "Zhenwei"
if len(sys.argv) >= 2:
    if sys.argv[1]:
        query = sys.argv[1]

# record the start time of the search.

start_time = datetime.datetime.now()
start_time_record = str(start_time)
print('star time: ' + start_time_record + '\n')

# open the file and read all the lines, put them in a list.
# divide the list to be lists
# assign the data in the list to these lists if present process in root process

with open('twitter.csv', encoding='utf-8') as f:
    if comm_rank == 0:
        for l in f:
            # data = f.readlines()
            data_chunk = [[] for _ in range(comm_size)]
            for index, key in enumerate(l):
                data_chunk[index % comm_size].append(key)
    else:
        # data = None
        data_chunk = None

    data = comm.scatter(data_chunk, root=0)

    query_one_chunk = Counter()
    users_one_chunk = Counter()
    topics_one_chunk = Counter()

    # for each data chunk,  count the query, users and topics
    # update the Counter class

    for line in data:
        query_one_line = re.findall(query, line.lower())
        query_one_chunk.update(query_one_line)

        user_one_line = re.findall('(?<=@)\w+', line.lower())
        users_one_chunk.update(user_one_line)

        topic_one_line = re.findall('(?<=#)\w+', line.lower())
        topics_one_chunk.update(topic_one_line)

    # collect all the data in different data chunk
    # combine them together
    data_query_all = comm.gather(query_one_chunk, root=0)
    data_user_all = comm.gather(users_one_chunk, root=0)
    data_topic_all = comm.gather(topics_one_chunk, root=0)

    if comm_rank == 0:
        end_time = datetime.datetime.now()
        end_time_record = str(end_time)
        print('\nend time: ' + end_time_record)

        print('\nQuery: ' + str(data_query_all))
        print('\nTop 10 users: ' + str(data_user_all))
        print('\nTop 10 topics: ' + str(data_topic_all))

        time_consuming = str(end_time - start_time)
        print('\nTime Consume: ' + time_consuming)
