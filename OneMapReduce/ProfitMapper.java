import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");

        String bookingStatus = parts[9];
        String arrivalMonth = parts[5];
        String arrivalYear = parts[4];
        String marketSegment = parts[7];

        try {
            if ("Not_Canceled".equalsIgnoreCase(bookingStatus) || Integer.parseInt(bookingStatus) >= 1) {
                int staysInWeekendNights = Integer.parseInt(parts[1]);
                int staysInWeekNights = Integer.parseInt(parts[2]);
                float avgPricePerRoom = Float.parseFloat(parts[8]);

                float income = (staysInWeekendNights + staysInWeekNights) * avgPricePerRoom;

                context.write(new Text(arrivalYear + "-" + arrivalMonth), new Text(String.valueOf(income)));
                context.write(new Text(arrivalMonth), new Text(String.valueOf(income)));
                context.write(new Text(arrivalMonth), new Text(marketSegment));
            }
        } catch (NumberFormatException e) {
            System.err.println("Error in parsing number: " + e.getMessage());
        }
    }
}

