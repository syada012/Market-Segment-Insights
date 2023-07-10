import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.text.DecimalFormat;

public class ProfitDriver extends Configured implements Tool {
    private static DecimalFormat df = new DecimalFormat("#.00");
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ProfitDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ProfitDriver <input path> <output path>");
            System.exit(-1);
        }

        // Run the jobs sequentially
        runJob(args, "Job1", IncomeMapper.class, IncomeReducer.class);
        runJob(args, "Job2", ProfitableMonthMapper.class, ProfitableMonthReducer.class);
        runJob(args, "Job3", AverageMonthMapper.class, AverageMonthReducer.class);
        runJob(args, "Job4", ProfitableSeasonMapper.class, ProfitableSeasonReducer.class);
        runJob(args, "Job5", PopularMarketSegmentMapper.class, PopularMarketSegmentReducer.class);

        // Combine the output into a single file
        FileSystem hdfs = FileSystem.get(getConf());
        Path outputPath = new Path(args[1] + "/combined");
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        FSDataOutputStream out = hdfs.create(outputPath);
        for (int i = 1; i <= 5; i++) {
            FSDataInputStream in = hdfs.open(new Path(args[1] + "/Job" + i + "/part-r-00000"));
            IOUtils.copyBytes(in, out, getConf(), false);
            in.close();
        }
        out.close();

        return 0;
    }

    private void runJob(String[] args, String jobName, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass) {
        try {
            Job job = Job.getInstance(getConf());
            job.setJobName(jobName);
            job.setJarByClass(ProfitDriver.class);
            job.setMapperClass(mapperClass);
            job.setReducerClass(reducerClass);

            if(jobName.equals("Job5")){
                job.setMapOutputValueClass(Text.class);
            } else {
                job.setMapOutputValueClass(FloatWritable.class);
            }

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);  // Change this line

            FileSystem hdfs = FileSystem.get(getConf());
            Path outputPath = new Path(args[1] + "/" + jobName);
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, outputPath);

            job.waitForCompletion(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class IncomeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String line = value.toString();
            String[] parts = line.split(",");
            String arrivalMonth = parts[5];
            String arrivalYear = parts[4];
            String bookingStatus = parts[9];
            try {
            if ("Not_Canceled".equalsIgnoreCase(bookingStatus) || Integer.parseInt(bookingStatus) >= 1) {
                float income = Float.parseFloat(parts[8]);
                context.write(new Text(arrivalYear + "-" + arrivalMonth), new FloatWritable(income));
            }
            } catch (NumberFormatException e) {
                System.err.println("Error in parsing number: " + e.getMessage());
          }
        }
    }

    public static class IncomeReducer extends Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalIncome = 0.0f;
            for (FloatWritable value : values) {
                totalIncome += value.get();
            }
            context.write(key, new Text(" $" + df.format(totalIncome)));
        }
    }

    public static class ProfitableMonthMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String line = value.toString();
            String[] parts = line.split(",");
            String arrivalMonth = parts[5];
            String arrivalYear = parts[4];
            String bookingStatus = parts[9];
            try{
            if ("Not_Canceled".equalsIgnoreCase(bookingStatus) || Integer.parseInt(bookingStatus) >= 1) {
                float income = Float.parseFloat(parts[8]);
                context.write(new Text(arrivalYear + "-" + arrivalMonth), new FloatWritable(income));
            }
            } catch (NumberFormatException e) {
                 System.err.println("Error in parsing number: " + e.getMessage());
         }
        }
    }

    public static class ProfitableMonthReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private Text mostProfitableYearMonth = new Text();
        private float maxIncome = Float.MIN_VALUE;

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalIncome = 0.0f;
            for (FloatWritable value : values) {
                totalIncome += value.get();
            }
            if (totalIncome > maxIncome) {
                maxIncome = totalIncome;
                mostProfitableYearMonth.set(key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //context.write(mostProfitableMonth, new FloatWritable(maxIncome));
            context.write(new Text("Most Profitable Month"), new Text(mostProfitableYearMonth + " $" + df.format(maxIncome)));
        }
    }

    public static class AverageMonthMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String line = value.toString();
            String[] parts = line.split(",");
            String arrivalMonth = parts[5];
            String bookingStatus = parts[9];
            try{
            if ("Not_Canceled".equalsIgnoreCase(bookingStatus) || Integer.parseInt(bookingStatus) >= 1) {
                float income = Float.parseFloat(parts[8]);
                context.write(new Text(arrivalMonth), new FloatWritable(income));
            }
            } catch (NumberFormatException e) {
                 System.err.println("Error in parsing number: " + e.getMessage());
         }
        }
    }

    public static class AverageMonthReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private Text mostProfitableMonth = new Text();
        private float maxAverageIncome = Float.MIN_VALUE;
        private Map<String, Float> totalIncomeMap = new HashMap<>();
        private Map<String, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalIncome = 0.0f;
            int count = 0;
            for (FloatWritable value : values) {
                totalIncome += value.get();
                count++;
            }
            totalIncomeMap.put(key.toString(), totalIncome);
            countMap.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String month : totalIncomeMap.keySet()) {
                float averageIncome = totalIncomeMap.get(month) / countMap.get(month);
                if (averageIncome > maxAverageIncome) {
                    maxAverageIncome = averageIncome;
                    mostProfitableMonth.set(month);
                }
            }
            //context.write(mostProfitableMonth, new FloatWritable(maxAverageIncome));
            context.write(new Text("Most Popular Month (Average)"), new Text(mostProfitableMonth + " $" + df.format(maxAverageIncome)));
        }
    }

    public static class ProfitableSeasonMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String line = value.toString();
            String[] parts = line.split(",");
            String arrivalMonth = parts[5];
            String bookingStatus = parts[9];
            try{
            if ("Not_Canceled".equalsIgnoreCase(bookingStatus) || Integer.parseInt(bookingStatus) >= 1) {
                String season = getSeason(arrivalMonth);
                float income = Float.parseFloat(parts[8]);
                context.write(new Text(season), new FloatWritable(income));
            }
            } catch (NumberFormatException e) {
                 System.err.println("Error in parsing number: " + e.getMessage());
         }
        }

        private String getSeason(String month) {
            switch (month.toLowerCase()) {
                case "January":
                case "February":
                case "March":
                    return "Winter";
                case "April":
                case "May":
                case "June":
                    return "Spring";
                case "July":
                case "August":
                case "September":
                    return "Summer";
                default:
                    return "Fall";
            }
        }
    }

    public static class ProfitableSeasonReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private Text mostProfitableSeason = new Text();
        private float maxIncome = Float.MIN_VALUE;

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalIncome = 0.0f;
            for (FloatWritable value : values) {
                totalIncome += value.get();
            }
            if (totalIncome > maxIncome) {
                maxIncome = totalIncome;
                mostProfitableSeason.set(key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //context.write(mostProfitableSeason, new FloatWritable(maxIncome));
            context.write(new Text("Most Profitable Season"), new Text(mostProfitableSeason + " $" + df.format(maxIncome)));
        }
    }

    public static class PopularMarketSegmentMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String line = value.toString();
            String[] parts = line.split(",");
            String arrivalMonth = parts[5];
            String bookingStatus = parts[9];
            try{
            if ("Not_Canceled".equalsIgnoreCase(bookingStatus) || Integer.parseInt(bookingStatus) >= 1) {
                String marketSegment = parts[7];
                context.write(new Text(arrivalMonth), new Text(marketSegment));
            }
            } catch (NumberFormatException e) {
                 System.err.println("Error in parsing number: " + e.getMessage());
         }
        }
    }

    public static class PopularMarketSegmentReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> countMap = new HashMap<>();
            for (Text value : values) {
                String marketSegment = value.toString();
                countMap.put(marketSegment, countMap.getOrDefault(marketSegment, 0) + 1);
            }
            String mostPopularMarketSegment = null;
            int maxCount = 0;
            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                if (entry.getValue() > maxCount) {
                    mostPopularMarketSegment = entry.getKey();
                    maxCount = entry.getValue();
                }
            }
            //context.write(key, new Text(mostPopularMarketSegment));
            context.write(new Text("Most Popular Market Segment for " + key.toString()), new Text(mostPopularMarketSegment));
        }
    }
}

