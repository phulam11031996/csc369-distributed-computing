package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstQueryPart1 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class MapperAccessLog extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String id = key.toString();
            String name = value.toString();
            context.write(new Text(id), new Text(name));
        }
    }

    // Mapper for messages file
    public static class MapperHostnameCountry extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String id = key.toString().replaceAll("\\s+","");
            String message = value.toString().replaceAll("\\s+","");
            context.write(new Text(id), new Text(message));
        }
    }

    // Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> name = new ArrayList();
            ArrayList<String> messages = new ArrayList();

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

}
