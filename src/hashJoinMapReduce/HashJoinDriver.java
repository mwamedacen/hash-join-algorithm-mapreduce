package hashJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hash join in MapReduce
 * 
 * The smaller table A is to be cached in memory and distributed over all
 * Mappers, table B will be read by Mappers and be joined with A in map phase.
 * 
 * The index of columns that will be joined should be set in the configuration
 * here.
 * 
 * Run with 5 parameters: (String smallTablePath, int smallTableColumnToJoin,
 * String bigTablePath, int bigTableColumnToJoin, String outputPath)
 * 
 * @author tangmm
 * 
 */
public class HashJoinDriver {

	public static void main(String[] args) throws Exception {
		// config for local post (to be modified when post changed)
/*		String fsDefaultName = "hdfs://localhost:9000/";
		Path inputPathA = new Path(fsDefaultName + "/user/tangmm/input/airport-runways.csv");
		Path inputPathB = new Path(fsDefaultName + "/user/tangmm/tableB");
		Path outputPath = new Path(fsDefaultName + "/user/tangmm/output-runway");
		int joinColA = 1;
		int joinColB = 0;
*/
		// config with parameter
		if (args.length != 5) {
			System.out.println("Error: run program with parameters!\n "
					+ "[String smallTablePath, int smallTableColumnToJoin,"
					+ "String bigTablePath, int bigTableColumnToJoin, String outputPath]");
			return;
		}
		Path inputPathA = new Path(args[0]);
		Path inputPathB = new Path(args[2]);
		Path outputPath = new Path(args[4]);
		int joinColA = Integer.parseInt(args[1]);
		int joinColB = Integer.parseInt(args[3]);

		int numReduceTasks = 5;

		Configuration conf = new Configuration();
		conf.setInt("joinColA", joinColA);
		conf.setInt("joinColB", joinColB);
		/** output in CSV format */
		conf.set("mapred.textoutputformat.separator", ", ");
		Job job = Job.getInstance(conf);
		job.setJarByClass(HashJoinDriver.class);

		/** add cache table A in HDFS */
		job.addCacheFile(inputPathA.toUri());

		FileInputFormat.setInputPaths(job, inputPathB);
		FileOutputFormat.setOutputPath(job, outputPath);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);

		job.setNumReduceTasks(numReduceTasks);

		job.setMapperClass(HashJoinMapper.class);
		job.setCombinerClass(HashJoinReducer.HashJoinCombiner.class); // inner
																		// class
		job.setPartitionerClass(HashJoinPartitioner.class);
		job.setReducerClass(HashJoinReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}