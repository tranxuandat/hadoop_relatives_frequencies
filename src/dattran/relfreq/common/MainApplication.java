package dattran.relfreq.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dattran.relfreq.hybrid.HybridMapper;
import dattran.relfreq.hybrid.HybridReducer;
import dattran.relfreq.pair.PairMapper;
import dattran.relfreq.pair.PairReducer;
import dattran.relfreq.stripe.StripeMapper;
import dattran.relfreq.stripe.StripeReducer;


public class MainApplication extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MainApplication(), args);
        System.exit(res);       
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: [input] [output] [type]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());

        int partern = Constants.DEFAULT_PARTERN;
        if (args.length > 2) {
        	String type = args[2];
        	if (type.equalsIgnoreCase("pair")) {
        		partern = Constants.PARTERN_PAIR;
        	} else if (type.equalsIgnoreCase("stripe")) {
        		partern = Constants.PARTERN_STRIPE;
        	} else if (type.equalsIgnoreCase("hybrid")) {
        		partern = Constants.PARTERN_HYBRID;
        	}
        }
        
        switch (partern) {
	        case Constants.PARTERN_PAIR:
	            job.setOutputKeyClass(TextPair.class);
	            job.setOutputValueClass(IntWritable.class);
	            job.setMapperClass(PairMapper.class);
	            job.setReducerClass(PairReducer.class);
	        	break;
	        case Constants.PARTERN_STRIPE:
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(CustomMapWritable.class);
	            job.setMapperClass(StripeMapper.class);
	            job.setReducerClass(StripeReducer.class);
	        	break;
	        case Constants.PARTERN_HYBRID:
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(CustomMapWritable.class);
	            job.setMapOutputKeyClass(TextPair.class);
	            job.setMapOutputValueClass(IntWritable.class);
	            job.setMapperClass(HybridMapper.class);
	            job.setReducerClass(HybridReducer.class);
	        	break;
        }
        job.setPartitionerClass(RelfreqPartitioner.class);
        job.setNumReduceTasks(Constants.NUMBER_OF_REDUCER);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(MainApplication.class);
        
     // Delete output if exists
		FileSystem hdfs = FileSystem.get(job.getConfiguration());
		if (hdfs.exists(new Path(args[1])))
			hdfs.delete(new Path(args[1]), true);
		
        job.waitForCompletion(true);
        return 0;
    }
}
