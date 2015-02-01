package hashJoin;

import java.io.IOException; 

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HashJoinReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 
		for (Text value : values) {
			context.write(key, value);
		}
	}
	
	/**
	 * Combiner
	 * @author tangmm
	 *
	 */
	static class HashJoinCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

}