package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondQueryOutput {

    public static final Class OUTPUT_KEY_CLASS = HostUrlCountPair.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, HostUrlCountPair, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strVal = value.toString().replaceAll("\\s+", " ").split(" ");
            String country = strVal[0];
            String url = strVal[1];
            int sum = Integer.valueOf(strVal[2]);


            context.write(new HostUrlCountPair(country, url, sum), new IntWritable(sum));
        }
    }
    
    public static class PartitionerImpl extends Partitioner<HostUrlCountPair, IntWritable> {
        @Override
        public int getPartition(HostUrlCountPair pair,
                IntWritable sum,
                int numberOfPartitions) {
            return Math.abs(pair.getCountryUrl().hashCode() % numberOfPartitions);
        }
    }

    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(HostUrlCountPair.class, true);
        }

        @Override
        public int compare(WritableComparable hostUrlCountPair1,
                WritableComparable hostUrlCountPair2) {
            HostUrlCountPair pair1 = (HostUrlCountPair) hostUrlCountPair1;
            HostUrlCountPair pair2 = (HostUrlCountPair) hostUrlCountPair2;

            return pair1.getCountryUrl().compareTo(pair2.getCountryUrl());
        }
    }

    // used to perform secondary sort on temperature
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(HostUrlCountPair.class, true);
        }

        @Override
        public int compare(WritableComparable hostUrlCountPair1,
                WritableComparable hostUrlCountPair2) {
            HostUrlCountPair pair1 = (HostUrlCountPair) hostUrlCountPair1;
            HostUrlCountPair pair2 = (HostUrlCountPair) hostUrlCountPair2;

            return pair1.compareTo(pair2);

        }
    }

    // output one line for each month, with the temperatures sorted for that month
    public static class ReducerImpl extends Reducer<HostUrlCountPair, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(HostUrlCountPair key,
                Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String currKey = key.getCountryUrl().toString();

            for (IntWritable val : values)
                context.write(new Text(currKey), new IntWritable(val.get()));
        }
    }

}
