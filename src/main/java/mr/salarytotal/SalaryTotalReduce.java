package mr.salarytotal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalaryTotalReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.计算总工资
        int total = 0;
        for (IntWritable v : values) {
            total += v.get();
        }
        //2.输出
        context.write(key, new IntWritable(total));
    }
}
