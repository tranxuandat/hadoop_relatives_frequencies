package dattran.relfreq.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;


public class CustomMapWritable extends HashMap<String, Double> implements Writable{

	@Override
	public void readFields(DataInput input) throws IOException {
		int size = input.readInt();
		clear();
		for (int i = 0 ; i < size ; i++) {
			String key = input.readUTF();
			Double value = input.readDouble();
			put(key, value);
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(size());
		for (String key : keySet()) {
			output.writeUTF(key);
			output.writeDouble(get(key));
		}
	}

}
