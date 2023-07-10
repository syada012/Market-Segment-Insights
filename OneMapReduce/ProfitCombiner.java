import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float totalIncome = 0.0f;
        for (Text value : values) {
            try {
                totalIncome += Float.parseFloat(value.toString());
            } catch (NumberFormatException e) {
                // This is a market segment, just pass it through
                context.write(key, value);
            }
        }
        context.write(key, new Text(String.valueOf(totalIncome)));
    }
}

