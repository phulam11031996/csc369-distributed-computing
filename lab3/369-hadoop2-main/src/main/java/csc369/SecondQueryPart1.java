package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondQueryPart1 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperAccessLog extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] sa = key.toString().split(" ");
            String ip = sa[0];
            String url = sa[6];

            context.write(new Text(ip), new Text(url));
        }

    }

    public static class MapperHostnameCountry extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String id = key.toString().replaceAll("\\s+", "") + " A";
            String message = value.toString().replaceAll("\\s+", "");

            context.write(new Text(id), new Text(message));
        }

    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values)
                context.write(key, val);
        }

    }

}
