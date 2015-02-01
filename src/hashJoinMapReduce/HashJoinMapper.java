package hashJoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * achat_bid ==> 0, buveur_bid ==> 0
 */
public class HashJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	HashMap<String, HashSet<String>> tableA = new HashMap<String, HashSet<String>>();

	public static final Log LOG = LogFactory.getLog(HashJoinMapper.class);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int joinColA = context.getConfiguration().getInt("joinColA", 0);

		// Get the cached file
		URI[] cacheFile = context.getCacheFiles();

		FSDataInputStream fsin = FileSystem.get(conf).open(new Path(cacheFile[0].getPath()));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsin));

		String line;
		System.out.println("SETUP");
		
		while ((line = br.readLine()) != null) {
			String[] row = line.toString().split(",");
			String joinKey = row[joinColA];

			// delete row[joinColA] from the line
			String newValue = StringUtils.join(row, ',', 0, joinColA);
			newValue = newValue.concat(",").concat(StringUtils.join(row, ',', joinColA + 1, row.length));

			this.addValue(tableA, joinKey, newValue);
			System.out.println("tableA: " + joinKey + "--" + newValue);
			LOG.info("tableA: " + joinKey + "--" + newValue);
		}
		br.close();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		System.out.println("MAPPER");
		int joinColB = context.getConfiguration().getInt("joinColB", 0);

		String[] row = value.toString().split(",");
		String joinKey = row[joinColB];

		// delete row[joinColB] from the line
		String valueB = StringUtils.join(row, ',', 0, joinColB);
		valueB = valueB.concat(",").concat(StringUtils.join(row, ',', joinColB + 1, row.length));

		System.out.println("tableB: " + joinKey + "--" + valueB);
		LOG.info("tableB: " + joinKey + "--" + valueB); 
		
		// inner join
		if (tableA.containsKey(joinKey)) {
			HashSet<String> valueSetA = tableA.get(joinKey);
			for (String valueA : valueSetA) { // 1-n join
				String newLine = valueA.concat(",").concat(valueB);
				
				System.out.println("Mapper: " + joinKey + "--" + newLine);
				
				context.write(new Text(joinKey), new Text(newLine));
			}
		}
	}

	/**
	 * insert a new value into the list of values associated to a key in tables
	 * 
	 * @param key
	 * @param newValue
	 */
	public void addValue(HashMap<String, HashSet<String>> table, String key, String newValue) {
		HashSet<String> currentValue = table.get(key);
		if (currentValue == null) {
			currentValue = new HashSet<String>();
			table.put(key, currentValue);
		}
		currentValue.add(newValue);
	}

}
