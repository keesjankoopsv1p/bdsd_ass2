package kees.mappers;

import kees.tools.ScentenceCleaner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BigramMapper {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        //simple function that takes a sentence and write all the bigrams to reducer
        public void map (LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            line = ScentenceCleaner.cleanSentence(line);

            //write bigrams
            for (int charPos = 0; charPos < line.length() -1; charPos++){
                Text bigram = new Text(line.substring(charPos, charPos + 2));
                context.write(bigram, one);
            }
        }
    }
}
