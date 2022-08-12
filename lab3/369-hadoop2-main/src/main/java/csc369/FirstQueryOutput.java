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

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().replaceAll("\\s+", " ").split(" ");

            Text hostname = new Text();
            hostname.set(sa[0]);
            IntWritable count = new IntWritable(Integer.valueOf(sa[1]));
            context.write(hostname, count);
        }
    }

    public static class CombinerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text hostname, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable count : counts)
                sum += count.get();

            Text hostnameCount = new Text();
            hostnameCount.set(hostname + " " + String.valueOf(sum));
            context.write(hostnameCount, new IntWritable(sum));
        }
    }

    // to sort in ascending order
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                WritableComparable wc2) {
            int hostnameCount1 = Integer.valueOf(wc1.toString().split(" ")[1]);
            int hostnameCount2 = Integer.valueOf(wc2.toString().split(" ")[1]);

            if (hostnameCount1 == hostnameCount2)
                return 0;
            else if (hostnameCount1 < hostnameCount2)
                return 1;
            else
                return -1;
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text hostname, Iterable<IntWritable> count,
                Context context) throws IOException, InterruptedException {
            Text currHostname = new Text(hostname.toString().split(" ")[0]);

            for (IntWritable el : count) {
                context.write(currHostname, el);
            }
        }
    }

}
