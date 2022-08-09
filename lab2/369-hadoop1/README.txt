Second Question: Very similar to question one, the output for the map function
is the HTTP instead of URL path.

Third Question: One MapReduce is used. The output for map function is hostname.
the output is the bytes sent to the client. An if statement is put at the
beginning for the map function; It checks for if the hostname is the give
hostname. The reduce function will aggregate all the hostname to one output with
the total bytes count.

Fourth Question: Two MapReduce are used. An if statement is the is the current

