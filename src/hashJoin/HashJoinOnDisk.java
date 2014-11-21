package hashJoin;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

public class HashJoinOnDisk  extends HashJoinImpl{

	public HashJoinOnDisk() throws IOException {
		super(); 
		// TODO Auto-generated method stub
	}
	
	public void buildHashTable() throws IOException
	{
		BufferedReader brTable1 = new BufferedReader(new FileReader(pathToTable1));
		String lineT1="";
		header1 = brTable1.readLine();
		while((lineT1 = brTable1.readLine()) != null) 
		{
			String[] rowT1 = lineT1.split(separator, -1);
			BufferedWriter part = new BufferedWriter(new FileWriter("data/part-"+rowT1[joinAttributeIndexT1].hashCode()+".csv",true));
			for(int i = 0; i < ITERATION; i++)
			{
				part.write(lineT1);
				part.newLine();
			}
			part.close();
		}
		brTable1.close();
	}
	
	public void hashJoin() throws IOException
	{
		BufferedReader  brTable2 = new BufferedReader(new FileReader(pathToTable2));
		String lineT2="";
		header2 = brTable2.readLine();
		header2 = removeJoinAttribute(header2,joinAttributeIndexT2);
		header =  moveJoinAttributeToFistPosition(header1, joinAttributeIndexT1) + "," + header2;
		
		BufferedWriter finalResultWriterBuffer =  new BufferedWriter(new FileWriter("output_onDisk.csv",true));
		finalResultWriterBuffer.write(header);
		finalResultWriterBuffer.newLine();
		while((lineT2 = brTable2.readLine()) != null)
		{
			String[] rowT2 = lineT2.split(separator, -1);
			lineT2 = removeJoinAttribute(lineT2,joinAttributeIndexT2);

			try{
				BufferedReader partitionBuffer = new BufferedReader(new FileReader("data/part-"+rowT2[joinAttributeIndexT2].hashCode()+".csv"));
				String hashLine ="";
				while((hashLine = partitionBuffer.readLine())!=null)
				{	
					String concat = moveJoinAttributeToFistPosition(hashLine, joinAttributeIndexT1) + "," + lineT2;
					finalResultWriterBuffer.write(concat);
					finalResultWriterBuffer.newLine();
				} 
				partitionBuffer.close();
			} catch(FileNotFoundException e){};
		}

		finalResultWriterBuffer.close();
		brTable2.close();

	}

}
