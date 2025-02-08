import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text year = new Text();
    private IntWritable temperature = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String date = fields[0];  // Assume first column is the Date
        String temp = fields[1]; // Assume second column is Temperature

        if (!temp.isEmpty()) {
            year.set(date.substring(0, 4));  // Extract year
            temperature.set(Integer.parseInt(temp));  // Parse temperature
            context.write(year, temperature);
        }
    }
}
