package kees;

import kees.enums.Language;
import kees.mappers.BigramMapper;
import kees.reducers.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class BigramCount extends Configured implements Tool{
    private static final Logger _logger = Logger.getLogger(BigramCount.class);
    private String pathString;

    public BigramCount(Language language){
        switch (language) {
            case NL :
                pathString = "/nlmatrix";
                break;
            case EN:
                pathString = "/enmatrix";
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        //create matrix
        Job job = Job.getInstance(getConf(), "wordcount");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(pathString));
        job.setMapperClass( BigramMapper.Map.class);
        job.setReducerClass(Reducer.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
