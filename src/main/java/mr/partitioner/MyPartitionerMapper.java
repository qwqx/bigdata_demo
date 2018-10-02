package mr.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyPartitionerMapper extends Mapper<LongWritable, Text, IntWritable, Emp> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();

        String[] words = data.split(",");
        Emp emp = new Emp();
        emp.setEmpno(Integer.parseInt(words[0]));
        emp.setEname(words[1]);
        emp.setJob(words[2]);
        emp.setMgr("".equals(words[3]) ? 0 : Integer.parseInt(words[3]));
        emp.setHiredate(words[4]);
        emp.setSal(Integer.parseInt(words[5]));
        emp.setComm("".equals(words[6]) ? 0 : Integer.parseInt(words[6]));
        emp.setDeptno(Integer.parseInt(words[7]));

        context.write(new IntWritable(emp.getDeptno()), emp);
    }
}
