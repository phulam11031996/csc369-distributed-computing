package csc369;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstQueryOutput {

    public static HashMap<String, String> ipAddressCountry = new HashMap<>();
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(10);

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().replaceAll("\\s+", " ").split(" ");


            Text hostname = new Text();
            hostname.set(sa[0]);
            IntWritable count = new IntWritable(Integer.valueOf(sa[1].toString()));
            context.write(hostname, count);
        }
    }

package csc369;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

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
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().replaceAll("\\s+", " ").split(" ");

            if (sa.length == 2) {
                ipAddressCountry.put(sa[0], sa[1]);
            } else {
                Text hostname = new Text();
                hostname.set(sa[0]);
                context.write(hostname, one);
            }
        }
    }
    
    // combiner executes on map nodes as a "mini-reducer"  (combines data based on key, before it is sent to reducer notes)
    public static class CombinerImpl extends Reducer<Text, SumCountPair, Text, SumCountPair> {
        @Override
        public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (SumCountPair el : values) {
                sum += el.getSum();
                count += el.getCount();
            }
            context.write(key, new SumCountPair(sum,count));
        }
    }


    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text word, Iterable<IntWritable> intOne,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();

            while (itr.hasNext()) {
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(new Text(ipAddressCountry.get(word.toString())), result);
        }
    }
    
}



    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text word, Iterable<IntWritable> intOne,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();

            while (itr.hasNext()) {
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(word, result);
        }
    }
    
}
