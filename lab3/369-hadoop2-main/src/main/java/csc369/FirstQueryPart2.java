package csc369;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstQueryPart2 {

    public static HashMap<String, String> ipAddressCountry = new HashMap<>();

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().replaceAll("\\s+", " ").split(" ");

            if (sa.length == 2) {
                ipAddressCountry.put(sa[0], sa[1]);
            } else {
                String hostname = sa[0];
                context.write(new Text(hostname), new IntWritable(1));
            }
        }

    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text word, Iterable<IntWritable> intOnes,
                Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable one : intOnes)
                sum += one.get();

            String country = ipAddressCountry.get(word.toString());
            context.write(new Text(country), new IntWritable(sum));
        }

    }

}
