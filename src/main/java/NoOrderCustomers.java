import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NoOrderCustomers extends JobMapReduce {

    public static class CustomerFileMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\|");
            Long custKey = Long.parseLong(tokens[0]);
            String custName = tokens[1];
//            System.out.println("CustomerFileMapper\t" + custKey + "\t" + custName);
            context.write(new LongWritable(custKey), new Text(custName));
        }
    }

    public static class OrdersFileMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\|");
            Long custKey = Long.parseLong(tokens[1]);
//            System.out.println("OrdersFileMapper\t" + custKey + "\t o");
            context.write(new LongWritable(custKey), new Text("o"));
        }
    }

    public static class NoOrderReducer extends Reducer<LongWritable, Text, Text, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<LongWritable,Text> map = new HashMap<>();
            for (Text value : values) {
//                System.out.println("Reducer evaluating\t" + key + "\t" + value);
                if(value.toString().startsWith("o")){
                    map.put(new LongWritable(0),new Text("found"));
                } else {
                    map.put(key, value);
                }
            }

            if(!map.containsKey(new LongWritable(0))){
                context.write(map.get(key),map.get(key));
            }

        }
    }

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "NoOrderCustomers");
        NoOrderCustomers.configureJob(job, this.input, this.input, this.output);
        return job.waitForCompletion(true);
    }

    public static void configureJob(Job job, String pathIn, String pathIn2, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(NoOrderCustomers.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
//        job.setCombinerClass(NoOrderCustomers.NoOrderReducer.class);
        job.setReducerClass(NoOrderCustomers.NoOrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(pathIn), TextInputFormat.class, CustomerFileMapper.class);
        MultipleInputs.addInputPath(job, new Path(pathIn2), TextInputFormat.class, OrdersFileMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }
}
