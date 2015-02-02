package hashJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HashJoinPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		int result = key.hashCode() & Integer.MAX_VALUE;
		
		System.out.println(key + "-------" + result);
		return (result) % numReduceTasks;
		
	}
}


