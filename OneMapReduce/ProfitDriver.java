import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ProfitDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ProfitDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ProfitDriver <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("Profit calculator");
        job.setJarByClass(ProfitDriver.class);
        job.setMapperClass(ProfitMapper.class);
        job.setCombinerClass(ProfitCombiner.class);  // Set the combiner class
        job.setReducerClass(ProfitReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);  // Correcting to match mapper output

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);  // Correcting to match reducer output

        FileSystem hdfs = FileSystem.get(getConf());
        if (hdfs.exists(new Path(args[1]))) {
            hdfs.delete(new Path(args[1]), true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

