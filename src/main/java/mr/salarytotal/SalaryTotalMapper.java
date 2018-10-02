package mr.salarytotal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalaryTotalMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.分词
        String data = value.toString();
        String[] words = data.split(",");

        //输出 （部门号，工资）
        context.write(new IntWritable(Integer.parseInt(words[7])),
                new IntWritable(Integer.parseInt(words[5])));
    }
}
