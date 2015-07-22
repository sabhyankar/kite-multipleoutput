package com.cloudera.sa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Formats;

import java.io.IOException;

/**
 * Created by Sameer Abhyankar on 7/21/15.
 */
public class KMODriver extends Configured implements Tool {

    private static final String HDFS_URI_PREFIX = "dataset:hdfs:";

    @Override
    public int run(String[] args) throws IOException,ClassNotFoundException,InterruptedException{

        if ( args.length < 3 ) {
            System.err.printf("Usage: %s [generic options] <input path> <output path> \n", getClass().getName());
            GenericOptionsParser.printGenericCommandUsage(System.err);

            for (int i = 0; i< args.length;i++)
                System.err.println("Got(" + i + "): " + args[i]);
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());

        LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        DatasetKeyMultipleOutputs.addNamedOutput(job,"WordCount1",
                                                Words.getClassSchema(),Words.class,
                                                HDFS_URI_PREFIX + args[2].toString() + "/WordCount1",
                CompressionType.Snappy);
        DatasetKeyMultipleOutputs.addNamedOutput(job,"WordCount2",
                Words.getClassSchema(),Words.class,
                HDFS_URI_PREFIX + args[2].toString() + "/WordCount2",
                CompressionType.Bzip2,
                Formats.AVRO);

        job.setMapperClass(MRWordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MRWordCountReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new GenericOptionsParser(new Configuration(),args).getConfiguration();
        int rc = ToolRunner.run(new KMODriver(),args);
        System.exit(rc);
    }
}
