package cz.cvut.bigdata.wordcount;

import cz.cvut.bigdata.cli.ArgumentParser;
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

/**
 * WordCount Example
 * <p/>
 * This is a very simple extension of basic WordCount
 * Example implemented using a new MapReduce API.
 */
public class WordCount extends Configured implements Tool {

	/** The main entry of the application. */
	public static void main(String[] arguments) throws Exception {
		System.exit(ToolRunner.run(new WordCount(), arguments));
	}

	/** This is where the MapReduce job is configured and being launched. */
	@Override
	public int run(String[] arguments) throws Exception {
		final ArgumentParser parser = new ArgumentParser("WordCount");

		parser.addArgument("input", true, true, "specify input directory");
		parser.addArgument("output", true, true, "specify output directory");
		parser.parseAndCheck(arguments);

		final Path inputPath = new Path(parser.getString("input"));
		final Path outputDir = new Path(parser.getString("output"));

		// Create configuration.
		final Configuration conf = getConf();

		// Using the following line instead of the previous would result in using the
		// default configuration settings. You would not have a change, for example,
		// to set the number of reduce tasks (to 5 in this example) by specifying:
		// -D mapred.reduce.tasks=5 when running the job from the console.
		//
		// Configuration conf = new Configuration(true);

		// Create job.
		final Job job = new Job(conf, "WordCount");
		job.setJarByClass(WordCountMapper.class);

		// Setup MapReduce.
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// Make use of a combiner - in this simple case it is the same as the reducer.
		job.setCombinerClass(WordCountReducer.class);

		// Sort the output words in reversed order.
		job.setSortComparatorClass(WordCountComparator.class);

		// Use custom partitioner.
		job.setPartitionerClass(WordCountPartitioner.class);

		// By default, the number of reducers is configured to be 1, similarly you can
		// set up the number of reducers with the following line.
		//
		// job.setNumReduceTasks(1);

		// Specify (key, value).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input.
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output.
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output directory (if exists).
		final FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		// Execute the job.
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
