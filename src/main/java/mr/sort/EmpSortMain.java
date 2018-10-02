package mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class EmpSortMain {
    public static void main(String[] args) throws Exception {
        args = new String[]{"D:\\大数据学习资料\\data\\emp.csv", "D:\\大数据学习资料\\data\\output\\sort"};

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(EmpSortMain.class);

        job.setMapperClass(EmpSortMapper.class);
        job.setMapOutputKeyClass(Emp.class );
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Emp.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
