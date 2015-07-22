package com.cloudera.sa;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Sameer Abhyankar on 7/20/15.
 */
public class MRWordCountReducer extends Reducer<Text,IntWritable,Void,Void> {

    private DatasetKeyMultipleOutputs mos;


    @Override
    public void setup(Context context) {
        mos = new DatasetKeyMultipleOutputs(context);
    }


    @Override
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{

        int count = 0;
        for (IntWritable v: values)
            count++;

        mos.write("WordCount1",new Words(key.toString(),count));
        mos.write("WordCount2",new Words(key.toString(),count));

    }

    @Override
    public void cleanup(Context context) throws IOException,InterruptedException {
        mos.close();
    }

}
