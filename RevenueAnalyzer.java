import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class RevenueAnalyzer {

    public static class RevenueAnalyzerMapper extends Mapper<Object, Text, Text, FloatWritable> {
        Text year_month = new Text();
        FloatWritable revenue = new FloatWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // if header, skip
            if (line.equals("stays_in_weekend_nights,stays_in_week_nights,arrival_year,arrival_month,market_segment_type,avg_price_per_room,booking_status")) {
                return;
            }

            String[] columns = line.split(",");
            if (columns.length >= 7) {
                int stays_in_weekend_nights, stays_in_week_nights, booking_status, total_bookings;
                String arrival_year, arrival_month;
                float avg_price_per_room, total_revenue;
                stays_in_weekend_nights = Integer.parseInt(columns[0].trim());
                stays_in_week_nights = Integer.parseInt(columns[1].trim());
                total_bookings = stays_in_weekend_nights + stays_in_week_nights;
                arrival_year = columns[2].trim();
                arrival_month = columns[3].trim();
                avg_price_per_room = Float.parseFloat(columns[5].trim());
                booking_status = Integer.parseInt(columns[6].trim());

                if (booking_status != 1) {
                    total_revenue = total_bookings * avg_price_per_room;
                }
                else {
                    total_revenue = 0; 
                }

                revenue.set(total_revenue);
                context.write(new Text(arrival_year + "-" + arrival_month), revenue);
            }
        }
    }

    public static class RevenueAnalyzerReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private List<ResultEntry> results_list;
        TreeMap<String, Float> result_two_year; 
        TreeMap<String, String> result_two_month; 
        float highest_revenue;
        String third_result; 

        protected void setup(Context context) {
            results_list = new ArrayList<>();
            result_two_year = new TreeMap<String, Float>();
            result_two_month = new TreeMap<String, String>();
            highest_revenue = -1 * Float.POSITIVE_INFINITY;
            third_result = ""; 
        }

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float total_revenue = 0;
            for (FloatWritable value : values) {
                total_revenue += value.get();
            }
            results_list.add(new ResultEntry(key.toString(), total_revenue));
            if (total_revenue >= highest_revenue){
                highest_revenue = total_revenue;
                third_result = key.toString();
            }

            String[] year_month = key.toString().split("-");
            String year, month;
            year = year_month[0];
            month = year_month[1];

            if (!result_two_year.containsKey(year) || total_revenue >= result_two_year.get(year)){
                result_two_year.put(year, total_revenue);
                result_two_month.put(year, month);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Ranking:"), null);
            Collections.sort(results_list, new ResultEntryComparator());
            int counter = 1;
            for (ResultEntry result : results_list) {
                context.write(new Text(counter + ". " + result.getKey()),null);
                counter++;
            }

            context.write(new Text("\nThe most popular month with the highest revenue:"), null);
            for (Map.Entry<String, Float> result : result_two_year.entrySet()) {
                String year = result.getKey();
                String result_key = year + "-" + result_two_month.get(year);
                context.write(new Text(result_key + ": " + result.getValue()), null);
            }

            context.write(new Text("\nOverall most profitable year and month:"), null);
            context.write(new Text(third_result + ", " + highest_revenue), null);
        }
    }

    private static class ResultEntry {
        private String key;
        private float revenue;

        public ResultEntry(String key, float revenue) {
            this.key = key;
            this.revenue = revenue;
        }

        public String getKey() {
            return key;
        }

        public float getRevenue() {
            return revenue;
        }
    }

    private static class ResultEntryComparator implements Comparator<ResultEntry> {
        public int compare(ResultEntry entry1, ResultEntry entry2) {
            // Sort in descending order of revenue
            return Float.compare(entry2.getRevenue(), entry1.getRevenue());
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Revenue Analyzer");

        job.setJarByClass(RevenueAnalyzer.class);
        job.setMapperClass(RevenueAnalyzerMapper.class);
        job.setReducerClass(RevenueAnalyzerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}