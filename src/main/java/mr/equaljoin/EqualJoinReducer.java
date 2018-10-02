package mr.equaljoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EqualJoinReducer extends Reducer<IntWritable, Text, Text,Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String dname = "";
        String empNameList = "";
        for (Text v : values) {
            int i = v.toString().indexOf("*");
            if (i >= 0) {
                dname = v.toString().substring(1);
            } else {
                empNameList = v.toString() + "," + empNameList;
            }
        }
        context.write(new Text(dname), new Text(empNameList));
    }
}
