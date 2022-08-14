package csc369;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Query5 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");

            String[] arrayDate = sa[3].substring(1).replace(":", "/").split("/");
            String month = arrayDate[1];
            String year = arrayDate[2];
            int dateInt = 0;

            if (month.equals("Jan"))
                dateInt = 1;
            if (month.equals("Feb"))
                dateInt = 2;
            if (month.equals("Mar"))
                dateInt = 3;
            if (month.equals("Apr"))
                dateInt = 4;
            if (month.equals("May"))
                dateInt = 5;
            if (month.equals("Jun"))
                dateInt = 6;
            if (month.equals("Jul"))
                dateInt = 7;
            if (month.equals("Aug"))
                dateInt = 8;
            if (month.equals("Sep"))
                dateInt = 9;
            if (month.equals("Oct"))
                dateInt = 10;
            if (month.equals("Dec"))
                dateInt = 11;
            if (month.equals("Nov"))
                dateInt = 12;
            dateInt += Integer.valueOf(year) * 100;

            Text hostname = new Text();
            hostname.set(String.valueOf(dateInt));
            context.write(hostname, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text hostname, Iterable<IntWritable> intOne,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();

            while (itr.hasNext()) {
                sum += itr.next().get();
            }
            result.set(sum);

            StringBuilder strHostname = new StringBuilder();
            String year = hostname.toString().substring(0, 4);
            String month = hostname.toString().substring(4);
            strHostname.append("Year: " + year + ", Month: " + month);

            Text newHostname = new Text();
            newHostname.set(strHostname.toString());
            context.write(newHostname, result);
        }
    }

}
