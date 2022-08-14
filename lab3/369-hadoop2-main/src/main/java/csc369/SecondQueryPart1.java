package csc369;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondQueryPart1 {

    public static HashMap<String, String> hostnameAndCountry = new HashMap<>();
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperAccessLog extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strVal = key.toString().replaceAll("\\s+", " ").split(" ");
            String token1 = strVal[0]; // hostname
            String token6 = strVal[6]; // url
            String hostnameAndUrl = token1 + " " + token6;

            context.write(new Text(hostnameAndUrl), new IntWritable(1));
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

    public static class JoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String[] hostnameAndUrl = key.toString().replaceAll("\\s+", " ").split(" ");
            String hostname = hostnameAndUrl[0];
            String url = hostnameAndUrl[1];
            String country = hostnameAndCountry.get(hostname);

            int sum = 0;

            for (IntWritable val : values)
                sum += val.get();

            context.write(new Text(country + " " + url), new IntWritable(sum));
        }
    }

}
