import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
 * since hadoop only support multiply mapper and one reducer,
 * I have to move two reduce job into two class.
 */
public class MatrixMultiplication1 {

    public static class CommonKeyMapper
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

            String[] data = line.split("\t");
            // label "0" stands for matrix M
            // label "1" stands for matrix N
            Text commonKey = new Text();
            Text diffValue = new Text();
            if (data[3].equals("0")){
                // commonKey is <j>
                commonKey.set(data[1]);
                // diffValue is <i><,><mij><,><0>
                diffValue.set(data[0]+","+data[2]+","+data[3]);
            }else if(data[3].equals("1")){
                // commonKey is <j>
                commonKey.set(data[0]);
                // diffValue is <k><,><njk><,><1>
                diffValue.set(data[1]+","+data[2]+","+data[3]);
            }
            context.write(commonKey,diffValue);
        }

    }


    public static class GenPairReducer
            extends Reducer<Text, Text, Text, FloatWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            /*
                key format:
                    <j>
                value format for matrix M:
                    <i><,><mij><,><0>
                value format for matrix N:
                    <k><,><nik><,><1>
            */
            String[] value;

            HashMap<Integer, Float> mapM = new HashMap<Integer, Float>();
            HashMap<Integer, Float> mapN = new HashMap<Integer, Float>();

            for (Text val : values){
                value = val.toString().split(",");
                if (value[2].equals("0")){
                    mapM.put(Integer.parseInt(value[0]), Float.parseFloat(value[1]));
                }else if(value[2].equals("1")) {
                    mapN.put(Integer.parseInt(value[0]), Float.parseFloat(value[1]));
                }
            }

            Text commonKey = new Text();
            float product = 0.0f;

            for (Integer i : mapM.keySet()){
                for (Integer k : mapN.keySet()){
                    // commonkey format:
                    //    <i><,><k>
                    commonKey.set(i+","+k);

                    product = mapM.get(i)*mapN.get(k);
                    context.write(commonKey, new FloatWritable(product));
                }
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

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "matrix multiply part 1");

        job.setJarByClass(MatrixMultiplication1.class);

        job.setMapperClass(CommonKeyMapper.class);
        // how to set two reducers for this job ?!!
        job.setReducerClass(GenPairReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
