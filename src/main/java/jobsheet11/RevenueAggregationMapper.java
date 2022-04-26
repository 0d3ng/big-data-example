package jobsheet11;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class RevenueAggregationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable revenueWriteable = new IntWritable(0);
    private Text kotaText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException
    {
        String line = value.toString();

        System.out.println("Ini adalah isi dari line");
        System.out.println(line);

        // Pecah dulu berdasarkan '-'
        String[] split = line.split("-");
        String kota = split[0];
        kota = kota.trim(); // Hilangkan spasi di depan/belakang

        // Cari revenue-nya
        String[] pendapatanSplit = split[1].split(" ");
        // Pendapatan ada di index paling akhir
        String pendapatan = pendapatanSplit[(pendapatanSplit.length - 1)];

        // Laporkan ke kolektor
        this.kotaText.set(kota);
        this.revenueWriteable.set(Integer.parseInt(pendapatan));
        output.collect(this.kotaText, this.revenueWriteable);
    }
}
