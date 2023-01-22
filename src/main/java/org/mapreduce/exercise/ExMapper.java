package org.mapreduce.exercise;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException{
        IntWritable one = new IntWritable(1);
        String line = value.toString();
        String[] csv = line.split(",", 3);
        Text pos = new Text();
        pos.set(csv[0]);

        output.collect(pos, one);
    }

}