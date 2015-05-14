package dattran.relfreq.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dattran.relfreq.common.TextPair;

public class HybridMapper extends Mapper<Object, Text, TextPair, IntWritable> {

    private Map<TextPair, Integer> maps;
    
    @Override
    protected void setup(
    		Mapper<Object, Text, TextPair, IntWritable>.Context context)
    		throws IOException, InterruptedException {
    	super.setup(context);
    	maps = new HashMap<TextPair, Integer>();
    }
    @Override
    public void map(Object key, Text value, Context output) throws IOException,
            InterruptedException {
    	
        String[] words = value.toString().split(" ");
        for (int i = 0 ; i < words.length; i++) {
        	for (int j = i+1 ; j < words.length; j++) {
        		if (words[j].equals(words[i])) {
        			break;
        		} else {
        			TextPair pair = new TextPair(words[i], words[j]);
        			if (!maps.containsKey(pair)) {
        				maps.put(pair, 1);
        			} else {
        				maps.put(pair, maps.get(pair) + 1);
        			}
        		}
        	}
        }
    }
    
    @Override
    protected void cleanup(
    		Mapper<Object, Text, TextPair, IntWritable>.Context context)
    		throws IOException, InterruptedException {
    	for (TextPair pair : maps.keySet()) {
    		context.write(pair, new IntWritable(maps.get(pair)));
    	}
    }
}