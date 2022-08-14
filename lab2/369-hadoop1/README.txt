Second Question: Very similar to question one, the output for the map function
is the HTTP instead of URL path.

Third Question: One MapReduce is used. The output for map function is hostname.
the output is the bytes sent to the client. An if statement is put at the
beginning for the map function; It checks for if the hostname is the give
hostname. The reduce function will aggregate all the hostname to one output with
the total bytes count.

Fourth Question: Two MapReduce are used. An if statement is used to check if the
current url contains the given url. Then the given url is the output for the
first map function and IntWritable of one are the second output. The second
MapReduce function is similar to the first query which sort the result.

Five Question: One MapReduces are used. I convert the year and month to an
integer from representation. So the reduce function can sort it in
chronologically order. The map second output are IntWritable. The shuffle will
sort the data. Then, in the reduce function, year and month will be converted
back to string representation and produce the output.

Sixth Question: Two MapReduces are used. Each log data are parsed to the year,
month, and day which are the output for the first map function. The second map's
output are IntWritable with bytes sent. The rest are similar to the first query.