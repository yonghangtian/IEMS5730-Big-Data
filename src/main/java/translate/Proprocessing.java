package translate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;



public class Proprocessing {

    public static class FormatMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // change value.toString() to line.
            String line = value.toString();
            //remove all soft hyphen symbols
            String rmSoftHyphen = line.replace("-","");
            // need to remove zero-width sysbols
            // not yet implemented

            String currFileName = getFileName(context);

            String lang = currFileName.split(".")[1];
            if (lang.equals("en")){
                try{

                    // using the tokenizer from Mases in perl, or the python tokenizer helper?
                    Process proc = Runtime.getRuntime().exec("echo"+"\""+line+"\""+ "| perl mosesdecoder/scripts/tokenizer/tokenizer.perl -l en");
                    proc.waitFor();

                    BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                    StringBuffer sb = new StringBuffer();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }else if (lang.equals("ch")){
                // using the tokenizer from Mases


            }

        }
    }

    private static String getFileName (MapContext context){
        return ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(Proprocessing.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
