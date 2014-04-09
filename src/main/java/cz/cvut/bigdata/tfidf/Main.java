package cz.cvut.bigdata.tfidf;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.tfidf.lines.LineNumberMapper;
import cz.cvut.bigdata.tfidf.lines.LineNumberPartitioner;
import cz.cvut.bigdata.tfidf.lines.LineNumberReducer;
import cz.cvut.bigdata.tfidf.terms.TermFrequencyMapper;
import cz.cvut.bigdata.tfidf.terms.TermFrequencyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Main entry-point of the TF-IDF application used
 * for executing MapReduce tasks.
 */
public class Main extends Configured implements Tool {

	public static void main(String[] arguments) throws Exception {
		System.exit(ToolRunner.run(new Main(), arguments));
	}

	private Configuration conf;
	private FileSystem hdfs;

	@Override
	public int run(String[] arguments) throws Exception {
		final ArgumentParser parser = new ArgumentParser("TF-IDF");

		parser.addArgument("input", true, true, "specify input directory");
		parser.addArgument("output", true, true, "specify output directory");
		parser.parseAndCheck(arguments);

		final Path wikiInput = new Path(parser.getString("input"));
		final String outputDir = parser.getString("output");

		conf = getConf();
		hdfs = FileSystem.get(conf);

		// input/output dirs
		final Path lines = new Path(outputDir, "lines");
		final Path terms = new Path(outputDir, "terms");

		//final Job lineNumberJob = prepareLineNumberJob(wikiInput, lines);
		final Job wordCountJob = prepareTermFrequencyJob(lines, terms);

		// TODO chain jobs and add other TF-IDF jobs

		// execute the jobs
		return wordCountJob.waitForCompletion(true) ? 0 : 1;
	}

	/** Create and setup the LineNumber job. */
	private Job prepareLineNumberJob(Path input, Path output) throws IOException {
		final Job job = new Job(conf, "LineNumber");

		// set MarReduce classes
		job.setJarByClass(LineNumberMapper.class);
		job.setMapperClass(LineNumberMapper.class);
		job.setReducerClass(LineNumberReducer.class);
		job.setPartitionerClass(LineNumberPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// setup input and output
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return job;
	}

	/** Create and setup the TermFrequency job. */
	private Job prepareTermFrequencyJob(Path input, Path output) throws IOException {
		final Job job = new Job(conf, "TermFrequency");

		// set MarReduce classes
		job.setJarByClass(TermFrequencyMapper.class);
		job.setMapperClass(TermFrequencyMapper.class);
		job.setReducerClass(TermFrequencyReducer.class);
		job.setCombinerClass(TermFrequencyReducer.class);
		job.setPartitionerClass(HashPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(TermDocWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// setup input and output
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return job;
	}
}
