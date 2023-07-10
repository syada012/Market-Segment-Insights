import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Float> monthYearIncomeMap = new HashMap<>();
    private Map<String, Float> monthIncomeMap = new HashMap<>();
    private Map<String, Integer> monthCountMap = new HashMap<>();
    private Map<String, Float> seasonIncomeMap = new HashMap<>();
    private Map<String, Map<String, Integer>> monthMarketSegmentCountMap = new HashMap<>();
    private String mostProfitableMonth;
    private float maxProfit;
    private String mostPopularMonth;
    private float maxAverageIncome;
    private String mostProfitableSeason;
    private float maxSeasonProfit;
    private DecimalFormat df = new DecimalFormat("#.00");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float totalIncome = 0.0f;
        Map<String, Integer> marketSegmentCountMap = new HashMap<>();
        for (Text value : values) {
            try {
                totalIncome += Float.parseFloat(value.toString());
            } catch (NumberFormatException e) {
                // This is a market segment
                String marketSegment = value.toString();
                marketSegmentCountMap.put(marketSegment, marketSegmentCountMap.getOrDefault(marketSegment, 0) + 1);
            }
        }

        String[] parts = key.toString().split("-");
        String monthYear = key.toString();
        if (parts.length == 2) {
            // This is a year-month key
            context.write(key, new Text(Float.toString(totalIncome)));
            monthYearIncomeMap.put(monthYear, monthYearIncomeMap.getOrDefault(monthYear, 0.0f) + totalIncome);
            monthIncomeMap.put(parts[1], monthIncomeMap.getOrDefault(parts[1], 0.0f) + totalIncome);
            monthCountMap.put(parts[1], monthCountMap.getOrDefault(parts[1], 0) + 1);

            // Calculate season income
            String season = getSeason(parts[1]);
            seasonIncomeMap.put(season, seasonIncomeMap.getOrDefault(season, 0.0f) + totalIncome);
        } else {
            // This is a month-only key
            monthIncomeMap.put(parts[0], monthIncomeMap.getOrDefault(parts[0], 0.0f) + totalIncome);
            monthCountMap.put(parts[0], monthCountMap.getOrDefault(parts[0], 0) + 1);

            // This is a month-marketSegment key
            monthMarketSegmentCountMap.put(parts[0], marketSegmentCountMap);
        }
    }

    private String getSeason(String month) {
        switch (month.toLowerCase()) {
            case "March":
            case "April":
            case "May":
                return "Spring";
            case "June":
            case "July":
            case "August":
                return "Summer";
            case "September":
            case "October":
            case "November":
                return "Fall";
            default:
                return "Winter";
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<String> months = Arrays.asList("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December");

        mostProfitableMonth = null;
        maxProfit = Float.MIN_VALUE;

        for (Map.Entry<String, Float> entry : monthYearIncomeMap.entrySet()) {
            String monthYear = entry.getKey();
            float totalIncome = entry.getValue();
            if (totalIncome > maxProfit) {
                mostProfitableMonth = monthYear;
                maxProfit = totalIncome;
            }
        }

        for (Map.Entry<String, Float> entry : monthIncomeMap.entrySet()) {
            String month = entry.getKey();
            float totalIncome = entry.getValue();
            int count = monthCountMap.get(month);
            float averageIncome = totalIncome / count;

            // Check if this month has higher average income
            if (averageIncome > maxAverageIncome) {
                mostPopularMonth = month;
                maxAverageIncome = averageIncome;
            }
        }

        // Calculate most profitable season
        for (Map.Entry<String, Float> entry : seasonIncomeMap.entrySet()) {
            if (entry.getValue() > maxSeasonProfit) {
                mostProfitableSeason = entry.getKey();
                maxSeasonProfit = entry.getValue();
            }
        }

        // Output the most profitable month across all years
        context.write(new Text("Most Profitable Month"), new Text(mostProfitableMonth + " ($" + df.format(maxProfit) + ")"));

        // Output the most popular month averaged across all years
        context.write(new Text("Most Popular Month (Average)"), new Text(mostPopularMonth + " ($" + df.format(maxAverageIncome) + ")"));

        // Output the most popular market segment for each month
        for (String month : months) {
            Map<String, Integer> marketSegmentCountMap = monthMarketSegmentCountMap.get(month);
            if (marketSegmentCountMap != null) {
                String mostPopularMarketSegment = null;
                int maxCount = 0;
                for (Map.Entry<String, Integer> marketSegmentEntry : marketSegmentCountMap.entrySet()) {
                    if (marketSegmentEntry.getValue() > maxCount) {
                        mostPopularMarketSegment = marketSegmentEntry.getKey();
                        maxCount = marketSegmentEntry.getValue();
                    }
                }
                context.write(new Text("Most Popular Market Segment for " + month), new Text(mostPopularMarketSegment));
            }
        }

        // Output the most profitable season
        context.write(new Text("Most Profitable Season"), new Text(mostProfitableSeason + " ($" + df.format(maxSeasonProfit) + ")"));
    }
}

