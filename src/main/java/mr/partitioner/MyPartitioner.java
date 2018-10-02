package mr.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<IntWritable, Emp> {
    @Override
    public int getPartition(IntWritable k2, Emp emp, int numTask) {
        return k2.get() % numTask;
    }
}
