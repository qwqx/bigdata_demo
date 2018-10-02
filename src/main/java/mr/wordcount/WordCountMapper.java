package mr.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text , LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] text = value.toString().split(" ");
        for (String t : text ) {
            context.write(new Text(t), new LongWritable(1));
        }
    }
}
