package mr.combiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgSalaryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        String data = value1.toString();

        String[] words = data.split(",");

        context.write(new Text("salary"), new DoubleWritable(Integer.parseInt(words[5])));
    }
}
