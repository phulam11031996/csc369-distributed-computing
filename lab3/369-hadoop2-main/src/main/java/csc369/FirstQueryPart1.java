package csc369;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstQueryPart1 {

    public static HashMap<String, String> hostnameAndCountry = new HashMap<>();
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperAccessLog extends Mapper<Text, Text, Text, IntWritable> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strVal = key.toString().replaceAll("\\s+", " ").split(" ");
            String token1 = strVal[0]; // hostname

            context.write(new Text(token1), new IntWritable(1));
        }

    }

    public static class MapperHostnameCountry extends Mapper<Text, Text, Text, IntWritable> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String hostname = key.toString().replaceAll("\\s+", "");
            String country = value.toString().replaceAll("\\s+", "");

            hostnameAndCountry.put(hostname.toString(), country.toString());
        }

    }

    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable hostname1,
                WritableComparable hostname2) {
            String country1 = hostnameAndCountry.get(hostname1.toString());
            String country2 = hostnameAndCountry.get(hostname2.toString());

            return country1.compareTo(country2);
        }

    }

    public static class JoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text hostnames, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String country = hostnameAndCountry.get(hostnames.toString());
            int sum = 0;

            for (IntWritable val : values)
                sum += val.get();

            context.write(new Text(country), new IntWritable(sum));
        }

    }

}
