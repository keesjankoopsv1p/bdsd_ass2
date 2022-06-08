package kees.mappers;

import kees.enums.Language;
import kees.tools.ScentenceCleaner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ClassifyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private static final String HDFS_ROOT_URL="hdfs://0.0.0.0:19000";
    private final HashMap<String, Double> _enMatrix;
    private final HashMap<String, Double> _nlMatrix;

    public ClassifyMapper() throws IOException {

        _enMatrix = this.getMatrix(Language.EN);
        _nlMatrix = this.getMatrix(Language.NL);
    }

    //Labels the language of the current line.
    public void map (LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        Double enScore = Double.valueOf(0);
        Double nlScore = Double.valueOf(0);
        String line = lineText.toString();
        line = ScentenceCleaner.cleanSentence(line);

        for (int charPos = 0; charPos < line.length() -1; charPos++){
            String bigram = line.substring(charPos, charPos + 2);
            Double nlBigramScore = _nlMatrix.get(bigram);
            Double enBigramScore = _enMatrix.get(bigram);
            if (nlBigramScore != null){
                nlScore = nlScore + nlBigramScore;
            }
            if (enBigramScore != null) {
                enScore = enScore + enBigramScore;
            }
        }

        if (nlScore > enScore){
            System.out.println(Language.NL + ": " + line);
            context.write(new Text(Language.NL.toString()), one);
        }
        if (enScore > nlScore){
            System.out.println(Language.EN + ": " + line);
            context.write(new Text(Language.EN.toString()), one);
        }
    }

    private HashMap<String, Double> getMatrix(Language language) throws IOException {
        switch (language){
            case EN:
                return readMatrixFile(HDFS_ROOT_URL + "/enmatrix/part-r-00000");
            case NL:
                return readMatrixFile(HDFS_ROOT_URL + "/nlmatrix/part-r-00000");
            default:
                return readMatrixFile(HDFS_ROOT_URL + "/nlmatrix/part-r-00000");
        }
    }

    //c
    private HashMap<String, Double> readMatrixFile(String filePath) throws IOException {
        HashMap<String, Integer> matrix = new HashMap<>();
        HashMap<String, Double> percentageMatrix;
        Integer totalFrequencies = 0;

        FileSystem fs = FileSystem.get(URI.create(filePath), new Configuration());
        InputStream in = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            in = fs.open(new Path(filePath));
            IOUtils.copyBytes(in, out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
        String text = out.toString();
        List<String> bigramArray = Arrays.asList(text.split("\n"));
        for (String bigramCombo : bigramArray){
            String bigram = bigramCombo.substring(0, 2);
            Integer frequence = Integer.valueOf(bigramCombo.substring(3));
            matrix.put(bigram, frequence);
            totalFrequencies = totalFrequencies +  frequence;
        }
        percentageMatrix = averageFrequencies(matrix, totalFrequencies);
        return percentageMatrix;
    }

    //converts matrix with integer frequencies to a matrix with double average
    private HashMap<String, Double> averageFrequencies(HashMap<String, Integer> matrix, Integer total) {
        HashMap<String, Double> result = new HashMap<>();
        for (String key: matrix.keySet()) {
            Double percentage = matrix.get(key) / (double) total;
            result.put(key, percentage);
        }
        return result;
    }
}
