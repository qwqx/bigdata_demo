package mr.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyPartitionerMain {
    public static void main(String[] args) throws Exception {
        args = new String[]{"D:\\大数据学习资料\\data\\emp.csv", "D:\\大数据学习资料\\data\\output\\partition"};

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MyPartitionerMain.class);

        job.setMapperClass(MyPartitionerMapper.class);
        job.setMapOutputKeyClass(IntWritable.class );
        job.setMapOutputValueClass(Emp.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Emp.class);

        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
