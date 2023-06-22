

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DatasetAppend {

    public static class AppendMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Set the entire line as the output value and save temporarily with a constant key
            outputValue.set(value);
            context.write(new Text("append_key"), outputValue);
        }
    }

    public static class AppendReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // Emit with an empty key and the original value to combine
                context.write(new Text(""), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Dataset Append");
        job.setJarByClass(DatasetAppend.class);
        job.setMapperClass(AppendMapper.class);
        job.setReducerClass(AppendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);  // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}