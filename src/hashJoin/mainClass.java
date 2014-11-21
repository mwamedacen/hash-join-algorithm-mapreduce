package hashJoin;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.commons.io.FileUtils;

public class mainClass {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub


		HashJoinImpl.ITERATION=1; //We use this variable to increase artificially the size of the data set
		//if ITERATION = 100, it means that the hash table is 100 times bigger than the original (100X values but the same number of keys) 
		Runtime runtime = Runtime.getRuntime();
		// Run the garbage collector
		runtime.gc();

		FileUtils.cleanDirectory(new File("data/"));
		FileUtils.deleteQuietly(new File("output_inMemory.csv"));
		FileUtils.deleteQuietly(new File("output_onDisk.csv"));
		
		System.out.println("iteration = " + HashJoinImpl.ITERATION);
		long time = System.currentTimeMillis();

		HashJoinImpl hashJoinOnDisk = new HashJoinOnDisk();
		hashJoinOnDisk.buildHashTable();
		hashJoinOnDisk.hashJoin();

		System.out.print("execution duration for on-disk method : ");
		System.out.println(System.currentTimeMillis()-time);
		System.out.print("memory used by on-disk method : ");
		System.out.println(runtime.totalMemory() - runtime.freeMemory());

		time = System.currentTimeMillis();

		HashJoinImpl hashJoinInMemory = new HashJoinInMemory();
		hashJoinInMemory.buildHashTable();
		hashJoinInMemory.hashJoin();

		System.out.print("execution duration for in-memory method : ");
		System.out.println(System.currentTimeMillis()-time);
		System.out.print("memory used by in-memory method : ");
		System.out.println(runtime.totalMemory() - runtime.freeMemory());

		

	}
}
