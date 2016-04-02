#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mpi4py import MPI
import sys
import datetime
from collections import Counter
import re

# initialize the MPI parameters
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

# initialize the query word, users can input query word as well.
# default query word is Melbourne University.
query = "Melbourne University "
if len(sys.argv) >= 2:
    if sys.argv[1]:
        query = sys.argv[1]

# record the start time of the search at the root.

start_time = datetime.datetime.now()
start_time_record = str(start_time)
if comm_rank == 0:
    print('star time: ' + start_time_record + '\n')


# define a function that can divide the file by reading several line once
# Then come back and continue read next several lines
def read_line_size(filename, size):
    while True:
        lines = filename.readlines(size)
        if not lines:
            break
        yield lines

# open the file, it will close the file after finishing all operations
# and throw no such file error automatically
with open('twitter.csv', encoding='utf-8') as f:

    # initialize glabal counters if it is root
    if comm_rank == 0:
        total_query = Counter()
        total_users = Counter()
        total_topic = Counter()

    # deal with the data chunk in a loop
    for singel_chunk in read_line_size(f, 80*1024*1024):
        if comm_rank == 0:
            data_chunk = [[] for _ in range(comm_size)]
            for index, key in enumerate(singel_chunk):
                data_chunk[index % comm_size].append(key)
        else:
            singel_chunk = None
            data_chunk = None

        # assign the data to every process
        data = comm.scatter(data_chunk, root=0)

        # record the start time for each node
        node_start_time = datetime.datetime.now()
        n_s_t_record = str(node_start_time)
        print("rank %d start at %s" %(comm_rank,n_s_t_record))

        # initialize counters for each process's data chunk
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

        # record the end time and time consume for each node
        node_end_time = datetime.datetime.now()
        n_e_t_record = str(node_end_time)
        n_time_consume = str(node_end_time - node_start_time)
        print('\nrank %d end at %s' %(comm_rank,n_e_t_record))
        print('Processing time for node %d is %s' %(comm_rank,n_time_consume))


        # collect all the data in different data chunk
        # combine them together
        data_query_all = comm.gather(query_one_chunk, root=0)
        data_users_all = comm.gather(users_one_chunk, root=0)
        data_topic_all = comm.gather(topics_one_chunk, root=0)

        # update the global counters at the root
        if comm_rank == 0:
            for counter_q in data_query_all:
                total_query.update(counter_q)

            for counter_u in data_users_all:
                total_users.update(counter_u)

            for counter_t in data_topic_all:
                total_topic.update(counter_t)

    # record the finish time and the outcome at root
    if comm_rank == 0:
        end_time = datetime.datetime.now()
        end_time_record = str(end_time)
        print('\nend time: ' + end_time_record)

        print('\nQuery: ' + str(total_query.most_common()))
        print('\nTop 10 users: ' + str(total_users.most_common(10)))
        print('\nTop 10 topics: ' + str(total_topic.most_common(10)))

        time_consuming = str(end_time - start_time)
        print('\nTotal Time Consume: ' + time_consuming)
