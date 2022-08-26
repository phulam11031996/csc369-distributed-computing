package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstQueryOutput {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] strKey = value.toString().replaceAll("\\s+", " ").split(" ");
            String country = strKey[0];
            int sum = Integer.valueOf(strKey[1]);

            context.write(new IntWritable(sum), new Text(country));
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable sum1,
                WritableComparable sum2) {
            IntWritable sum01 = (IntWritable) sum1;
            IntWritable sum02 = (IntWritable) sum2;

            return -1 * sum01.compareTo(sum02);
        }

    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            for (Text val : values)
                context.write(val, key);
        }
    }

}
