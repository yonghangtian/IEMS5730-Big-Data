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


/**
 * @author tianyh
 *
 * This is an bad implementation of matrix multiplication. DO NOT USE IT AGAIN!!!
 */
public class MultiplyMatrix {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        /*
             value format for matrix M:
                <i> <TAB> <j> <TAB> <mij> <TAB> <0>
             value format for matrix N:
                <j> <TAB> <k> <TAB> <njk> <TAB> <1>
        */
            // change value to string.
            String line = value.toString();
            // read configuration from context.
            Configuration conf = context.getConfiguration();
            int rowOfM = Integer.parseInt(conf.get("i"));
            int colOfN = Integer.parseInt(conf.get("k"));
            Text commonKey = new Text();
            Text diffValue = new Text();

            String[] data = line.split("\t");
            // label "0" stands for matrix M
            if (data[3].equals("0")){
                for (int k = 1; k <= colOfN; k++) {
                    //key: <i>,<k>
                    commonKey.set(data[0] + "," + k);
                    //value: <label>,<j>,<mij>
                    diffValue.set(data[3]+","+data[1]+","+data[2]);
                    //context.write(Text, Text);
                    context.write(commonKey, diffValue);
                }
            }
            //label "1" stands for matrix N
            else if(data[3].equals("1")){
                for (int i = 1; i <= rowOfM; i++){
                    // key: <i>,<k>
                    commonKey.set(i + "," + data[1]);
                    // value: <label>,<j>,<njk>
                    diffValue.set(data[3]+","+data[0]+","+data[2]);
                    //context.write(Text, Text);
                    context.write(commonKey, diffValue);
                }
            }
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

        job.setJarByClass(MultiplyMatrix.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(VectorMultiplyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
