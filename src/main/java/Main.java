import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LocalMapReduce");


        if (args[0].equals("-totalPrice")) {
            TotalPrice.configureJob(job, args[1], args[2]);
        }
        else if (args[0].equals("-noOrderCustomers")) {
            NoOrderCustomers.configureJob(job, args[1], args[2], args[3]);
        }

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Main driver = new Main();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}
