package dattran.relfreq.stripe;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dattran.relfreq.common.CustomMapWritable;

public class StripeReducer extends Reducer<Text, CustomMapWritable, Text, CustomMapWritable> {
    @Override
    public void reduce(Text key, Iterable<CustomMapWritable> values, Context output)
            throws IOException, InterruptedException {
    	CustomMapWritable sumMap = new CustomMapWritable();
        for(CustomMapWritable value: values){
            for (String valueKey : value.keySet()) {
            	double valueInt = value.get(valueKey);
            	if (!sumMap.containsKey(valueKey)) {
            		sumMap.put(valueKey, valueInt);
            	} else {
            		double valueSumInt = sumMap.get(valueKey);
            		sumMap.put(valueKey, valueSumInt + valueInt);
            	}
            }
        }
        int sum = 0;
        for (String value : sumMap.keySet()) {
        	double valueInt = sumMap.get(value);
        	sum += valueInt;
        }
        CustomMapWritable finalMap = new CustomMapWritable();
        for (String value : sumMap.keySet()) {
        	double valueInt = sumMap.get(value);
        	finalMap.put(value, (double)valueInt/sum);
        }
        output.write(key, finalMap);
    }
}