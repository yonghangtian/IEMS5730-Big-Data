package cmsc5741;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;


public class FindSimilarUser {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


        }
    }

    public static class VectorMultiplyReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] value;

            HashMap<Integer, Float> mapM = new HashMap<Integer, Float>();
            HashMap<Integer, Float> mapN = new HashMap<Integer, Float>();

            for (Text val : values){
                value = val.toString().split(",");
                // put value from M(with label "0") and N(with label "1") into different hashMap
                if (value[0].equals("0")){
                    mapM.put(Integer.parseInt(value[1]),Float.parseFloat(value[2]));
                } else{
                    mapN.put(Integer.parseInt(value[1]),Float.parseFloat(value[2]));
                }
            }
            int sharedJ = Integer.parseInt(context.getConfiguration().get("j"));
            float result = 0.0f;
            // value from M and N
            float m_ij;
            float n_jk;
            // since this j here is not the original j, this j is the index of pair in hashmap.
            // (we dont care about the order of these pairs.)
            for (int j = 0; j < sharedJ; j++){
                m_ij = mapM.containsKey(j) ? mapM.get(j) : 0.0f;
                n_jk = mapN.containsKey(j) ? mapN.get(j) : 0.0f;
                result += m_ij * n_jk;
            }
            // since the original matrix is sparse, we dont need to print out 0's.
            if (result != 0.0f){
                context.write(new Text(key.toString()),
                        new Text(Float.toString(result)));
            }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: matrixMultiply <in> <out>");
            System.exit(2);
        }
        /* we need to find out i,j,k first.
           these conf is for small-dataset!!!!!
                i = 16907; j = 610; k = 16907.

           these conf is for median-dataset!!!!
                i = 193609; j = 610; k = 193609

           these conf is for large-dataset!!!!
                i = 130642; j = 7120; k = 130642


         */
        conf.set("i", "193609");
        conf.set("j", "610");
        conf.set("k", "193609");
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "matrix multiply");

        job.setJarByClass(FindSimilarUser.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(VectorMultiplyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
