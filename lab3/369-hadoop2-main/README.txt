First Query: 
    FirstQueryPart1:
        Two map functions. Because the information about hostname
        and country is small, the first map takes input as hostname_country.csv file and
        stores hostname and country in a hashmap. A limitation of this method is if
        the information for hostname and country gets large, then the implementation
        doesn't work. The second mapper function's output is the hostname and
        IntWritable of one.

        Group comparator: To aggregate all the database of country name

        Reduce: Use the info in the map to output texts including country and sum count
    
    SecondQueryPart2: Just to aggregate and sum all the counts, but the output
        is not sort
    
    SecondQueryOutput:
        Mapper: output for key is IntWritable to have the shuffle sort based on
        the number of requests. Value is country

        Sort comparator: to override the sorting and have it sort from the highest
        to lowest

        Reducer: Key output is country and value output is the total request for each country
    
Second Query:
    SecondQueryPart1: Very similar to FirstQueryPart1, but the reducer's key output
    include the country and URL
    
    SecondQueryPart2: To aggregate all the countries and URLs and calculate their sums

    SecondQueryOutput:
        Mapper: Mapper's key output is a class with country, URL, and the sum of count (for sorting).
        Mapper's value output is IntWritable.

        Group comparator: to override grouping for HostUrlCountPair. The grouping is
        based on country and URL

        Sort comparator: to first sort elements by country and then the sum of counts
        from highest to lowest

        Reducer: Reducer's key output is Texts of country and URL and value are
        IntWritable of the sum of counts

        
    