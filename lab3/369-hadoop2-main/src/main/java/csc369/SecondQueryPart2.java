package csc369;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondQueryPart2 {

    public static HashMap<String, String> ipAddressCountry = new HashMap<>();

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value
                    .toString()
                    .replaceAll("\\s+", " ")
                    .split(" ");

            if (sa.length == 3) {
                ipAddressCountry.put(sa[0], sa[2]);
            } else {
                String hostname = sa[0] + " " + sa[1];
                context.write(new Text(hostname), new IntWritable(1));
            }
        }

    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text word, Iterable<IntWritable> intOnes,
                Context context) throws IOException, InterruptedException {
            String[] sa = word
                    .toString()
                    .replaceAll("\\s+", " ")
                    .split(" ");

            int sum = 0;

            for (IntWritable one : intOnes)
                sum += one.get();

            String countryAndUrl = ipAddressCountry.get(sa[0]) + " " + sa[1];
            context.write(new Text(countryAndUrl), new IntWritable(sum));
        }

    }

}
