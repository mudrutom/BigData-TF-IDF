package cz.cvut.bigdata.tfidf;

import cz.cvut.bigdata.cli.ArgumentParser;
import cz.cvut.bigdata.tfidf.docs.InverseDocFrequencyMapper;
import cz.cvut.bigdata.tfidf.docs.InverseDocFrequencyPartitioner;
import cz.cvut.bigdata.tfidf.docs.InverseDocFrequencyReducer;
import cz.cvut.bigdata.tfidf.lines.LineNumberMapper;
import cz.cvut.bigdata.tfidf.lines.LineNumberPartitioner;
import cz.cvut.bigdata.tfidf.lines.LineNumberReducer;
import cz.cvut.bigdata.tfidf.terms.TermFrequencyMapper;
import cz.cvut.bigdata.tfidf.terms.TermFrequencyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Main entry-point of the TF-IDF application used for executing
 * MapReduce jobs. The main task is computation of TF-IDF index
 * for the corpus consisting of Czech wikipedia articles
 * <p/>
 * The jobs are executed in the following order:
 * <ul>
 *     <li><i>LineNumber</i> job - numbering of lines (documents)</li>
 *     <li><i>TermFrequency</i> job - computation of term-document frequencies</li>
 *     <li><i>InverseDocFrequency</i> job - computation of inverse-document frequencies</li>
 * </ul>
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
		final Path tfidf = new Path(outputDir, "tf-idf");

		// create the TF-IDF jobs
		final ControlledJob lineNumberJob = prepareLineNumberJob(wikiInput, lines);
		final ControlledJob termFrequencyJob = prepareTermFrequencyJob(lines, terms);
		final ControlledJob inverseDocFrequencyJob = prepareInverseDocFrequencyJob(terms, tfidf);

		// chain the jobs together
		final JobControl control = new JobControl("TF-IDF");
		control.addJob(lineNumberJob);
		termFrequencyJob.addDependingJob(lineNumberJob);
		control.addJob(termFrequencyJob);
		inverseDocFrequencyJob.addDependingJob(lineNumberJob);
		inverseDocFrequencyJob.addDependingJob(termFrequencyJob);
		control.addJob(inverseDocFrequencyJob);

		// execute the jobs
		control.run();
		return control.allFinished() ? 0 : 1;
	}

	/** Create and setup the LineNumber job. */
	private ControlledJob prepareLineNumberJob(Path input, Path output) throws IOException {
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

		return new ControlledJob(job, null);
	}

	/** Create and setup the TermFrequency job. */
	private ControlledJob prepareTermFrequencyJob(Path input, Path output) throws IOException {
		final Job job = new Job(conf, "TermFrequency");

		// set MarReduce classes
		job.setJarByClass(TermFrequencyMapper.class);
		job.setMapperClass(TermFrequencyMapper.class);
		job.setReducerClass(TermFrequencyReducer.class);
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

		return new ControlledJob(job, null);
	}

	/** Create and setup the InverseDocFrequency job. */
	private ControlledJob prepareInverseDocFrequencyJob(Path input, Path output) throws IOException {
		final Job job = new Job(conf, "InverseDocFrequency");

		// set MarReduce classes
		job.setJarByClass(InverseDocFrequencyMapper.class);
		job.setMapperClass(InverseDocFrequencyMapper.class);
		job.setReducerClass(InverseDocFrequencyReducer.class);
		job.setPartitionerClass(InverseDocFrequencyPartitioner.class);

		// set the key-value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TermDocFreqWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// setup input and output
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		// delete output directory (if it exists)
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		return new ControlledJob(job, null);
	}
}
