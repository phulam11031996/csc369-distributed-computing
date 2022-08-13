package csc369;

import java.io.IOException;

import javax.lang.model.util.ElementScanner14;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondQueryOutput {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().replaceAll("\\s+", " ").split(" ");

            String countryAndUrl = sa[0] + " " + sa[1];
            int count = Integer.valueOf(sa[2]);

            context.write(new Text(countryAndUrl), new IntWritable(count));
        }
    }

    public static class CombinerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text countriesAndUrls, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable count : counts)
                sum += count.get();

            String countryUrlAndCount = countriesAndUrls.toString() + " " + String.valueOf(counts);
            context.write(new Text(countryUrlAndCount), new IntWritable(sum));
        }
    }

    // // to sort in ascending order
    // public static class GroupingComparator extends WritableComparator {
    //     public GroupingComparator() {
    //         super(Text.class, true);
    //     }

    //     @Override
    //     public int compare(WritableComparable text1,
    //             WritableComparable text2) {

    //         String country1 = text1.toString().split(" ")[0];
    //         String country2 = text2.toString().split(" ")[0];

    //         int count1 = Integer.valueOf(text1.toString().split(" ")[2]);
    //         int count2 = Integer.valueOf(text2.toString().split(" ")[2]);

    //         if (country1.equals(country2)) {
    //             if (count1 < count2)
    //                 return 1;
    //             else if (count1 > count2)
    //                 return -1;
    //             else
    //                 return 0;
    //         } else {
    //             return country1.compareTo(country2);
    //         }
    //     }
    // }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text countriesUrlsAndCounts, Iterable<IntWritable> counts,
                Context context) throws IOException, InterruptedException {
            
            for (IntWritable count : counts)
                context.write(countriesUrlsAndCounts, count);
                    

            // String[] sa = countriesUrlsAndCounts.toString().split(" ");
            // String countryAndUrl = sa[0] + " " + sa[1];

            // int sum = 0;
            // for (IntWritable el : counts)
            //     sum += el.get();

            // context.write(new Text(countryAndUrl), new IntWritable(sum));
        }

    }

}
