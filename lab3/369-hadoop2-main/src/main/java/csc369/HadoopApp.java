package csc369;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
            System.exit(-1);

        } else if ("FirstQueryPart1".equalsIgnoreCase(otherArgs[0])) {
            MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
                    KeyValueTextInputFormat.class, FirstQueryPart1.MapperAccessLog.class);
            MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
                    KeyValueTextInputFormat.class, FirstQueryPart1.MapperHostnameCountry.class);
            job.setGroupingComparatorClass(FirstQueryPart1.GroupingComparator.class);
            job.setReducerClass(FirstQueryPart1.JoinReducer.class);
            job.setOutputKeyClass(FirstQueryPart1.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(FirstQueryPart1.OUTPUT_VALUE_CLASS);
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        } else if ("FirstQueryOutput".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(FirstQueryOutput.ReducerImpl.class);
            job.setMapperClass(FirstQueryOutput.MapperImpl.class);
            job.setSortComparatorClass(FirstQueryOutput.SortComparator.class);
            job.setOutputKeyClass(FirstQueryOutput.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(FirstQueryOutput.OUTPUT_VALUE_CLASS);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        } else if ("SecondQueryPart1".equalsIgnoreCase(otherArgs[0])) {
            MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
                    KeyValueTextInputFormat.class, SecondQueryPart1.MapperAccessLog.class);
            MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
                    KeyValueTextInputFormat.class, SecondQueryPart1.MapperHostnameCountry.class);
            job.setReducerClass(SecondQueryPart1.JoinReducer.class);
            job.setOutputKeyClass(SecondQueryPart1.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(SecondQueryPart1.OUTPUT_VALUE_CLASS);
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        } else if ("SecondQueryPart2".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(SecondQueryPart2.ReducerImpl.class);
            job.setMapperClass(SecondQueryPart2.MapperImpl.class);
            job.setOutputKeyClass(SecondQueryPart2.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(SecondQueryPart2.OUTPUT_VALUE_CLASS);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        } else if ("SecondQueryOutput".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(SecondQueryOutput.ReducerImpl.class);
            job.setMapperClass(SecondQueryOutput.MapperImpl.class);

            job.setGroupingComparatorClass(SecondQueryOutput.GroupingComparator.class);
            job.setSortComparatorClass(SecondQueryOutput.SortComparator.class);

            job.setOutputKeyClass(SecondQueryOutput.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(SecondQueryOutput.OUTPUT_VALUE_CLASS);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        } else {
            System.out.println("Unrecognized job: " + otherArgs[0]);
            System.exit(-1);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
