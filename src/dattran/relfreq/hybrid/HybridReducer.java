package dattran.relfreq.hybrid;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dattran.relfreq.common.CustomMapWritable;
import dattran.relfreq.common.TextPair;

public class HybridReducer extends Reducer<TextPair, IntWritable, Text, CustomMapWritable> {
	private int currentSum = 0;
	private CustomMapWritable currentMap;
	private String currentTerm;
	
	@Override
	protected void setup(
			Reducer<TextPair, IntWritable, Text, CustomMapWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		currentSum = 0;
		currentMap = new CustomMapWritable();
		currentTerm = null;
	}
    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context output)
            throws IOException, InterruptedException {
    	if (!key.getLeft().equals(currentTerm)) {
    		if (currentTerm != null) {
	    		for (String keyMap : currentMap.keySet()) {
	    			currentMap.put(keyMap, currentMap.get(keyMap)/currentSum);
	    		}
	    		output.write(new Text(currentTerm), currentMap);
    		}
    		currentSum = 0;
    		currentTerm = key.getLeft();
    		currentMap = new CustomMapWritable();
    	}
		if (!currentMap.containsKey(key.getRight())) {
			currentMap.put(key.getRight(), 0.0);
		}
		for (IntWritable value : values) {
			int i = value.get();
			currentMap.put(key.getRight(), currentMap.get(key.getRight()) + i);
			currentSum += i;
		}
    }
    @Override
    protected void cleanup(
    		Reducer<TextPair, IntWritable, Text, CustomMapWritable>.Context output)
    		throws IOException, InterruptedException {
		for (String keyMap : currentMap.keySet()) {
			currentMap.put(keyMap, currentMap.get(keyMap)/currentSum);
		}
		output.write(new Text(currentTerm), currentMap);
    }
}