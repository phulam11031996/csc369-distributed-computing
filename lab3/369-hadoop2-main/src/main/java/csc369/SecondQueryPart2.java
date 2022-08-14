package csc369;

import java.io.IOException;
import java.util.HashMap;

import javax.naming.InitialContext;
import javax.sound.midi.SysexMessage;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondQueryPart2 {

    public static HashMap<String, String> hostnameAndCountry = new HashMap<>();
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strVal = value.toString().replaceAll("\\s+", " ").split(" ");
            String token1 = strVal[0]; // country
            String token2 = strVal[1]; // url
            int token3 = Integer.valueOf(strVal[2]); // sum

            context.write(new Text(token1 + " " + token2), new IntWritable(token3));
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            context.write(new Text(key.toString()), new IntWritable(sum));
        }
    }

}
