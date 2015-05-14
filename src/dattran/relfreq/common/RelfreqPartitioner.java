package dattran.relfreq.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelfreqPartitioner extends Partitioner<TextPair, IntWritable>{

	@Override
	public int getPartition(TextPair key, IntWritable value, int numReduceTasks) {
		int partitioner = key.getLeft().hashCode()%numReduceTasks;
		return partitioner;
	}

}
