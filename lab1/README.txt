To run program: install the latest version of python3 then type 'make run' in
terminal.

program design: I chose to use range-based partition on event id for the program.
It is easy to implement and also don't over load any partition since all event
ids are sorted in ascending order. I divide the data into 9 partitions with an
index file which contains the range for event id partitions. Looking for event
id (the first query) and search for query within a range (the second query) are
quite efficient. However, I had to access all the partition for solve the last
three query.

program implementation:
read_and_store_file and write_to_partition_files will store all of the json data
into one list, separate the 9 partitions and also automatically calculate and
create an index.json file. find_partition_by_event_id will find the name of the
partition file using the by using the index.json file and the provided event id.
first_query will return the string of json data for the query #1. Same for the
other 4 queries.

