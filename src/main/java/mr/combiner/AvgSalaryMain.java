package mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgSalaryMain {
    public static void main(String[] args) throws Exception {
        args = new String[]{"D:\\大数据学习资料\\data\\emp.csv", "D:\\大数据学习资料\\data\\output\\combiner"};

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(AvgSalaryMain.class);

        job.setMapperClass(AvgSalaryMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(AvgSalaryReduce.class);

        job.setReducerClass(AvgSalaryReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
