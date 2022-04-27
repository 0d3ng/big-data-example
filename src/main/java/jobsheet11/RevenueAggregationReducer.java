package jobsheet11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class RevenueAggregationReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    private final static Logger log = LogManager.getLogger(RevenueAggregationReducer.class);
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException
    {
        log.info("----------------");
        log.info("Ini dari reducer");
        log.info("Key: " + key.toString());
        log.info("----------------");
        int sum = 0;
        while (values.hasNext())
        {
            IntWritable currentRevenue = values.next();
            log.info("Isi dari values saat ini: " + currentRevenue.get());
            sum += currentRevenue.get();
        }
        output.collect(key, new IntWritable(sum));
    }
}
