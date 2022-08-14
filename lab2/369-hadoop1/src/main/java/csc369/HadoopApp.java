package csc369;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.out.println("Expected parameters: <job class> <input dir> <output dir>");
            System.exit(-1);
        } else if ("Query1Part1".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query1Part1.ReducerImpl.class);
            job.setMapperClass(Query1Part1.MapperImpl.class);
            job.setOutputKeyClass(Query1Part1.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query1Part1.OUTPUT_VALUE_CLASS);
        } else if ("Query1Part2".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query1Part2.ReducerImpl.class);
            job.setMapperClass(Query1Part2.MapperImpl.class);
            job.setOutputKeyClass(Query1Part2.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query1Part2.OUTPUT_VALUE_CLASS);
        } else if ("Query2Part1".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query2Part1.ReducerImpl.class);
            job.setMapperClass(Query2Part1.MapperImpl.class);
            job.setOutputKeyClass(Query2Part1.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query2Part1.OUTPUT_VALUE_CLASS);
        } else if ("Query2Part2".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query2Part2.ReducerImpl.class);
            job.setMapperClass(Query2Part2.MapperImpl.class);
            job.setOutputKeyClass(Query2Part2.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query2Part2.OUTPUT_VALUE_CLASS);
        } else if ("Query3".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query3.ReducerImpl.class);
            job.setMapperClass(Query3.MapperImpl.class);
            job.setOutputKeyClass(Query3.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query3.OUTPUT_VALUE_CLASS);
        } else if ("Query4Part1".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query4Part1.ReducerImpl.class);
            job.setMapperClass(Query4Part1.MapperImpl.class);
            job.setOutputKeyClass(Query4Part1.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query4Part1.OUTPUT_VALUE_CLASS);
        } else if ("Query4Part2".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query4Part2.ReducerImpl.class);
            job.setMapperClass(Query4Part2.MapperImpl.class);
            job.setOutputKeyClass(Query4Part2.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query4Part2.OUTPUT_VALUE_CLASS);
        } else if ("Query5".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query5.ReducerImpl.class);
            job.setMapperClass(Query5.MapperImpl.class);
            job.setOutputKeyClass(Query5.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query5.OUTPUT_VALUE_CLASS);
        } else if ("Query6Part1".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query6Part1.ReducerImpl.class);
            job.setMapperClass(Query6Part1.MapperImpl.class);
            job.setOutputKeyClass(Query6Part1.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query6Part1.OUTPUT_VALUE_CLASS);
        } else if ("Query6Part2".equalsIgnoreCase(otherArgs[0])) {
            job.setReducerClass(Query6Part2.ReducerImpl.class);
            job.setMapperClass(Query6Part2.MapperImpl.class);
            job.setOutputKeyClass(Query6Part2.OUTPUT_KEY_CLASS);
            job.setOutputValueClass(Query6Part2.OUTPUT_VALUE_CLASS);
        } else {
            System.out.println("Unrecognized job: " + otherArgs[0]);
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
