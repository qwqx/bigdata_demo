package mr.salarytotal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryTotalMain {

    public static void main(String[] args) throws Exception {

        args = new String[]{"D:\\大数据学习资料\\data\\emp.csv", "D:\\data\\output\\totalsalary.csv"};

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SalaryTotalMain.class);

        job.setMapperClass(SalaryTotalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SalaryTotalReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
