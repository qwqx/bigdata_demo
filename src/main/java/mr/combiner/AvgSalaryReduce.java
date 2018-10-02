package mr.combiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgSalaryReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text k3, Iterable<DoubleWritable> v3,Context context) throws IOException, InterruptedException {
        double total = 0;
        int count = 0;

        for(DoubleWritable salary:v3){
            total = total + salary.get();
            count ++;
        }

        context.write(new Text("The avg salary is :"), new DoubleWritable(total/count));
    }
}
