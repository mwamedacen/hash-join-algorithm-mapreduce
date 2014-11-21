package hashJoin;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;

public class HashJoinInMemory extends HashJoinImpl{
	private HashMap<String, LinkedList<String>> hashTableForJoining;
	

	public HashJoinInMemory() throws IOException {
		super();
		// TODO Auto-generated method stub
	}

	public void buildHashTable() throws IOException
	{
		BufferedReader brTable1 = new BufferedReader(new FileReader(pathToTable1));
		String lineT1="";
		hashTableForJoining = new HashMap<String, LinkedList<String>>();
		
		header1 = brTable1.readLine();
		
		//Starts from 2nd line thanks to the previous call 
		while((lineT1 = brTable1.readLine()) != null) 
		{
			String[] rowT1 = lineT1.split(separator, -1);
			for(int i = 0; i < ITERATION; i++)
			{
				LinkedList<String> updatedList;
				LinkedList<String> currentList = hashTableForJoining.get(rowT1[joinAttributeIndexT1]);
				updatedList = (currentList != null) ? currentList : new LinkedList<String>();
				updatedList.add(lineT1);
				hashTableForJoining.put(rowT1[joinAttributeIndexT1], updatedList);
			}
		}
		brTable1.close();

	}

	public void hashJoin() throws IOException
	{
		BufferedReader  brTable2 = new BufferedReader(new FileReader(pathToTable2));
		String lineT2="";

		header2 = brTable2.readLine(); //we fetch the header
		header2 = removeJoinAttribute(header2,joinAttributeIndexT2); //we apply on it the removeJoinAttrubte 
		//operation to prevent from redundancy
		header =  moveJoinAttributeToFistPosition(header1, joinAttributeIndexT1) + "," + header2;
		
		BufferedWriter brTableWrite = new BufferedWriter(new FileWriter("output_inMemory.csv"));
		//we create an new header composed of header1 and header2 by respecting the laws of .csv files
		//we also make sure that term isn'it a comma
		
		brTableWrite.append(header + "\n");
		while((lineT2 = brTable2.readLine()) != null)
		{
			String[] rowT2 = lineT2.split(separator, -1);
			LinkedList<String> listWithRedundancy = hashTableForJoining.get(rowT2[joinAttributeIndexT2]);
			lineT2 = removeJoinAttribute(lineT2,joinAttributeIndexT2); //we remove the joinAttribute from all rows to prevent from redundancy

			if(listWithRedundancy != null)
			{
				for(String eachHashedValue : listWithRedundancy)
				{
					if(eachHashedValue != null) // for the given joinAttribute, this checks if the value of T2 matches with the current value stored in the hashtable
					{
						//don't use an object finalResult
						brTableWrite.write(moveJoinAttributeToFistPosition(eachHashedValue, joinAttributeIndexT1) + "," + lineT2);    //if so we create a new row composed of the content of the hashtable (from T1) and T2
						brTableWrite.newLine();
					}
				}
			}
		}
		hashTableForJoining.clear();
		brTable2.close();
		brTableWrite.close();
	}
}
