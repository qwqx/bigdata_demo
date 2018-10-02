package mr.equaljoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EqualJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();

        String[] words = data.split(",");

        if(words.length == 3){
            context.write(new IntWritable(Integer.parseInt(words[0])), new Text("*"+words[1]));
        }else{
            context.write(new IntWritable(Integer.parseInt(words[7])), new Text(words[1]));
        }
    }
}
