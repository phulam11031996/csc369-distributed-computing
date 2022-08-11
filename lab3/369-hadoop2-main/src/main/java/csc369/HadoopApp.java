package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 3) {
        System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
        System.exit(-1);
    } else if ("FirstQueryPart1".equalsIgnoreCase(otherArgs[0])) {
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
                    KeyValueTextInputFormat.class, FirstQueryPart1.MapperAccessLog.class );
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
                    KeyValueTextInputFormat.class, FirstQueryPart1.MapperHostnameCountry.class ); 
        job.setReducerClass(FirstQueryPart1.JoinReducer.class);
        job.setOutputKeyClass(FirstQueryPart1.OUTPUT_KEY_CLASS);
        job.setOutputValueClass(FirstQueryPart1.OUTPUT_VALUE_CLASS);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        
    } else if ("FirstQueryPart2".equalsIgnoreCase(otherArgs[0])) {
        job.setReducerClass(FirstQueryPart2.ReducerImpl.class);
        job.setMapperClass(FirstQueryPart2.MapperImpl.class);
        job.setOutputKeyClass(FirstQueryPart2.OUTPUT_KEY_CLASS);
        job.setOutputValueClass(FirstQueryPart2.OUTPUT_VALUE_CLASS);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        
    } else if ("FirstQueryOutput".equalsIgnoreCase(otherArgs[0])) {
        job.setReducerClass(FirstQueryOutput.ReducerImpl.class);
        job.setMapperClass(FirstQueryOutput.MapperImpl.class);
        job.setOutputKeyClass(FirstQueryOutput.OUTPUT_KEY_CLASS);
        job.setOutputValueClass(FirstQueryOutput.OUTPUT_VALUE_CLASS);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    } else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
        job.setReducerClass(AccessLog.ReducerImpl.class);
        job.setMapperClass(AccessLog.MapperImpl.class);
        job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
        job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    } else {
        System.out.println("Unrecognized job: " + otherArgs[0]);
        System.exit(-1);
    }
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
