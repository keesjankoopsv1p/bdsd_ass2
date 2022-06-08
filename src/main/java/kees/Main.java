package kees;

import kees.enums.Language;
import org.apache.hadoop.util.ToolRunner;

public class Main {

    /* works with 2 different commands
    *
    * train command: used to build the matrix that is used to classify sentences
    * takes 3 arguments: train, *language* (either 'en' or 'nl'), *input_path*: hdfs path to text file used to build the matrix
    *
    * classify command: used to classify languages in either dutch or english
    * takes 3 arguments: classify, *input_path*: hdfs path of the text file to classfiy, *output_path* hdfs path with the results
    */
    public static void main( String[] args ) throws Exception {
        String command = args[0];
        switch (command){
            case "train":
                switch (args[1]){
                    case "en":
                        int resEn = ToolRunner.run(new BigramCount(Language.EN), args);
                        System.out.println("English matrix build");
                        System.exit(resEn);
                        break;
                    case "nl":
                        int resNL = ToolRunner.run(new BigramCount(Language.NL), args);
                        System.out.println("Dutch matrix build");
                        System.exit(resNL);
                }
                break;
            case "classify":
                int resClassify = ToolRunner.run(new Classifier(), args);
                break;
        }
    }
}
