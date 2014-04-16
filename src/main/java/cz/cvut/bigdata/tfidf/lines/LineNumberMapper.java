package cz.cvut.bigdata.tfidf.lines;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Receives <b>(long, 'text')</b> pairs of a byte offsets and a texts.
 * Performs identity function while counting number of lines for each
 * reducer, according to the LineNumberPartitioner partitioning. When
 * finished, a cumulative sum of line counters is sent to a particular
 * reducer, using special message with negative key value. Then it emit
 * the final cumulative sum using the zero key value.
 */
public class LineNumberMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private final LineNumberPartitioner partitioner = new LineNumberPartitioner();

	private int reducersNum;
	private int[] lineCounters;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// init the line counters
		reducersNum = context.getNumReduceTasks();
		lineCounters = new int[reducersNum];
		Arrays.fill(lineCounters, 0);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0L) {
			// skip the first line
			return;
		}
		context.write(key, value);

		// increment line counter of the responsible reducer
		final int reducer = partitioner.getPartition(key, value, reducersNum);
		lineCounters[reducer]++;
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		final LongWritable reducer = new LongWritable();
		final Text count = new Text();

		int cumSum = 0;
		for (int i = 0; i < reducersNum; i++) {
			// send cumulative sum to a particular reducer
			reducer.set(-(i + 1));
			count.set(String.valueOf(cumSum));
			context.write(reducer, count);

			// compute cumulative sum
			cumSum += lineCounters[i];
		}

		// send the overall number of lines
		count.set(String.valueOf(cumSum));
		context.write(new LongWritable(0L), count);
	}
}
