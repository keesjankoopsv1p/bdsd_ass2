package kees;

import kees.mappers.ClassifyMapper;
import kees.reducers.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Classifier extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Job classifyJob = Job.getInstance(getConf(), "classify");
        classifyJob.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(classifyJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(classifyJob, new Path(args[2]));
        classifyJob.setMapperClass( ClassifyMapper.class );
        classifyJob.setReducerClass(Reducer.Reduce.class);
        classifyJob.setOutputKeyClass(Text.class);
        classifyJob.setOutputValueClass(IntWritable.class);

        return classifyJob.waitForCompletion(true) ? 0 : 1;
    }
}
