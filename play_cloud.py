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

query = "Unimelb"
if len(sys.argv) >= 2:
    if sys.argv[1]:
        query = sys.argv[1]

# record the start time of the search.

start_time = datetime.datetime.now()
start_time_record = str(start_time)
print('star time: ' + start_time_record + '\n')

# initialize the whole counter of three parameters
total_query = Counter()
total_users = Counter()
total_topic = Counter()


# define a function that can chunk a string into several sub-strings.
def chunk_string(string, num):
    ave = len(string) / float(num)
    data_chunks = []
    tail = 0.0
    while tail < len(string):
        data_chunks.append(string[int(tail):int(tail+ave)])
        tail += ave
    return data_chunks

# open the file and read all the lines, put them in a list.
# divide the list to be lists
# assign the data in the list to these lists if present process in root process
with open('twitter.csv', encoding='utf-8') as f:
    for l in f:

        if comm_rank == 0:
            data_chunk = chunk_string(l, comm_size)
        else:
            data_chunk = None
            l = None

        data = comm.scatter(data_chunk, root=0)

        query_one_chunk = Counter()
        users_one_chunk = Counter()
        topic_one_chunk = Counter()

        query_one_line = re.findall(query, data.lower())
        query_one_chunk.update(query_one_line)

        user_one_line = re.findall('(?<=@)\w+', data.lower())
        users_one_chunk.update(user_one_line)

        topic_one_line = re.findall('(?<=#)\w+', data.lower())
        topic_one_chunk.update(topic_one_line)

        data_query_all = comm.gather(query_one_chunk, root=0)
        data_user_all = comm.gather(users_one_chunk, root=0)
        data_topic_all = comm.gather(topic_one_chunk, root=0)

        for counter_q in data_query_all:
            total_query.update(counter_q)

        for counter_u in data_user_all:
            total_users.update(counter_u)

        for counter_t in data_topic_all:
            total_topic.update(counter_t)

if comm_rank == 0:
    end_time = datetime.datetime.now()
    end_time_record = str(end_time)
    print( '\nend time: ' + end_time_record)

    print('\nQuery: ' + str(total_query))
    print('\nTop 10 users: ' + str(total_users.most_common(10)))
    print('\nTop 10 topics: ' + str(total_topic.most_common(10)))

    time_consuming = str(end_time - start_time)
    print('\nTime Consume: ' + time_consuming)
