package mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountMain {

    static Logger log = LoggerFactory.getLogger(WordCountMain.class);
    static {

        try {

            System.load("D:\\开发工具安装包\\hadoop-2.7.2\\bin\\hadoop.dll");

        } catch (UnsatisfiedLinkError e) {

            System.err.println("Native code library failed to load.\n" + e);

            System.exit(1);

        }

    }
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\开发工具安装包\\hadoop-2.7.2");
        args = new String[]{"D:\\data\\wordcount.txt", "D:\\data\\a.txt"};

        Job job = new Job(new Configuration());
        job.setJarByClass(WordCountMain.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
