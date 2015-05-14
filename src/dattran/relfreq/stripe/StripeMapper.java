package dattran.relfreq.stripe;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dattran.relfreq.common.CustomMapWritable;

public class StripeMapper extends Mapper<Object, Text, Text, CustomMapWritable> {

    private Map<Text, CustomMapWritable> maps;
    
    @Override
    protected void setup(
    		Mapper<Object, Text, Text, CustomMapWritable>.Context context)
    		throws IOException, InterruptedException {
    	super.setup(context);
    	maps = new HashMap<Text, CustomMapWritable>();
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
            		Text keyText = new Text(words[i]);
            		if (!maps.containsKey(keyText)) {
        				maps.put(keyText, new CustomMapWritable());
        			}
            		CustomMapWritable tmp = maps.get(keyText);
            		String valueText = words[j];
            		if (tmp.containsKey(valueText)) {
            			double val = tmp.get(valueText);
            			tmp.put(valueText, val + 1);
            		} else {
            			tmp.put(valueText, 1.0);
            		}
        		}
        	}
        }
    }
    
    @Override
    protected void cleanup(
    		Mapper<Object, Text, Text, CustomMapWritable>.Context context)
    		throws IOException, InterruptedException {
    	for (Text key : maps.keySet()) {
    		context.write(key, maps.get(key));
    	}
    }
}