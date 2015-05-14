package dattran.relfreq.pair;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import dattran.relfreq.common.TextPair;

public class PairReducer extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {
	private static int totalValue = 0;
    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context output)
            throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value: values){
            count+= value.get();
        }
        if (key.getRight().equals("*")) {
        	totalValue = count;
        } else {
        	output.write(key, new DoubleWritable((1.0*count)/totalValue));
        }
    }
}