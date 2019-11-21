import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TotalPrice extends JobMapReduce {

    public static class OrderPriorityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] tokens = value.toString().split("\\|");
                SimpleDateFormat format =new SimpleDateFormat("yyyy-MM-dd");
                Date baseDate = format.parse("1993-10-01");
                Date orderDate = format.parse(tokens[4]);

                if (orderDate.compareTo(baseDate) >= 0){
                    Text priorityOrder = new Text(tokens[5]);
                    Double price = Double.parseDouble(tokens[3]);
                    context.write(priorityOrder, new DoubleWritable(price));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class OrderPriorityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("heyy");
        Configuration configuration = new Configuration();
        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "TotalPrice");
        TotalPrice.configureJob(job, this.input, this.output);
        // Let's run it!
        return job.waitForCompletion(true);
    }

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(TotalPrice.class);
        job.setMapperClass(TotalPrice.OrderPriorityMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(TotalPrice.OrderPriorityReducer.class);
        job.setReducerClass(TotalPrice.OrderPriorityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));

    }
}
