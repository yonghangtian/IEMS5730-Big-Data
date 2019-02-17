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

/**
 * @author tianyh
 */
public class MatrixMultiplication2 {


    public static class DoNothingMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");

            Text strKey = new Text(data[0]);
            float val = Float.parseFloat(data[1]);

            context.write(strKey,new FloatWritable(val));
        }
    }


    public static class FloatSumReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            float result = 0.0f;

            for (FloatWritable val : values){
                result += val.get();
            }

            if (result != 0.0f){
                context.write(key,new FloatWritable(result));
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
        Job job = new Job(conf, "matrix multiply part 2");

        job.setJarByClass(MatrixMultiplication2.class);

        job.setMapperClass(DoNothingMapper.class);
        // how to set two reducers for this job ?!!
        job.setReducerClass(FloatSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

