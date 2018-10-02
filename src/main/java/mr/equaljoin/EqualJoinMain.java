package mr.equaljoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EqualJoinMain {
    static Logger logger = LoggerFactory.getLogger(EqualJoinMain.class);
    public static void main(String[] args) throws Exception {
        args = new String[]{"D:\\大数据学习资料\\data", "D:\\大数据学习资料\\data\\output\\equaljoin"};
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(EqualJoinMain.class);

        job.setMapperClass(EqualJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(EqualJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPathFilter(job, new PathFilter() {
            public boolean accept(Path path) {
                logger.info("**********************{},{}", path.getName(), path.depth());
                if(path.depth() <= 4) {
                    return true;
                }
                return false;
            }
        }.getClass());

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
