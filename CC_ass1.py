#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
from collections import Counter
import re


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

# open the file and read each line of it, counting the query word, users and topics.

with open('twitter.csv', 'r', encoding='utf-8') as f:

    # initialize the counters by using the Counter class.
    query_frequency = Counter()
    user_frequency = Counter()
    topic_frequency = Counter()

    # for every line in the file, count the query times, users and topics
    for line in f:
        # print(line)
        query_one_line = re.findall(query, line.lower())
        # query_frequency += len(query_one_line)
        query_frequency.update(query_one_line)

        user_one_line = re.findall('(?<=@)\w+', line.lower())
        # user_frequency += len(user_one_line)
        user_frequency.update(user_one_line)

        topic_one_line = re.findall('(?<=#)\w+', line.lower())
        # topic_frequency += len(topic_one_line)
        topic_frequency.update(topic_one_line)

print('query : ' + str(query_frequency.most_common()))
print('\ntop 10 users searched: ' + str(user_frequency.most_common(10)))
print('\ntop 10 topics searched: ' + str(topic_frequency.most_common(10)))

end_time = datetime.datetime.now()
end_time_record = str(end_time)
print('\nend time: ' + end_time_record)

time_consuming = str(end_time - start_time)
print('\n Time consume: ' + time_consuming)
