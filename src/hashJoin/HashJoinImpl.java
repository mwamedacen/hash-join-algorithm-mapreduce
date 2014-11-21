package hashJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public abstract class HashJoinImpl implements HashJoin {
	public static int ITERATION = 1;
	protected String pathToTable1;
	protected String pathToTable2;
	protected String joinAttributeNameT1;
	protected String joinAttributeNameT2;
	protected final String separator = "\\s*,\\s*";
	protected int joinAttributeIndexT1;
	protected int joinAttributeIndexT2;
	protected String header1, header2, header = "";
	

	public HashJoinImpl() throws IOException
	{
		//Process these 3 functions sequentially and wait till the end of the last
		askForTables(); // fill paths to tables and 
		askForJoinAttributes(); // fill joinAttributes and joinAttributeIndexes

		joinAttributeIndexT1 = fetchIndexOfJoinAttribute(pathToTable1,joinAttributeNameT1);
		joinAttributeIndexT2 = fetchIndexOfJoinAttribute(pathToTable2,joinAttributeNameT2);
	}

	protected int fetchIndexOfJoinAttribute(String pathToTable, String joinAttributeName) throws IOException
	{
		BufferedReader brTable = new BufferedReader(new FileReader(pathToTable));
		String line = "";
		String[] temp;
		if((line = brTable.readLine()) != null) 
		{
			temp = line.split(separator, -1);
			for(int i = 0, l = temp.length; i < l; i++)
			{
				if(temp[i].equals(joinAttributeName)) 
				{
					brTable.close();
					return i;
				}
			}
		}
		brTable.close();
		return -1;
	}

	protected void askForTables() {
		pathToTable1 = "airport-frequencies.csv";
		pathToTable2 = "airports.csv";

		//pathToTable1 = "address.csv";
		//pathToTable2 = "people.csv";
	}
	protected void askForJoinAttributes() {
		//		//System.out.println("Merci de choisir l'attribut sur lequel vous souhaitez effectuer la jointure: ");
		//		Scanner userInput = new Scanner(System.in);
		//		joinAttributeNameT1 = userInput.next();
		//		//System.out.println("Merci d'indiquer le nom de l'autre attribut: ");
		//		joinAttributeNameT2 = userInput.next();

		joinAttributeNameT1 = "\"airport_ident\"";
		joinAttributeNameT2 = "\"ident\"";
		//joinAttributeNameT1 = "id";
		//joinAttributeNameT2 = "id_adresse";
	}

	protected String removeJoinAttribute(String row, int indexOfElementToRemove)
	{
		String[] rowTab = row.split(separator,-1);
		ArrayList<String> a = new ArrayList<String>(Arrays.asList(rowTab));
		a.remove(indexOfElementToRemove);
		return StringUtils.join(a, ",");
	}

	protected String moveJoinAttributeToFistPosition(String row, int indexOfElementToRemove)
	{
		String[] rowBis = row.split(separator, -1);
		ArrayList<String> updatedList = new ArrayList<String>(Arrays.asList(rowBis));
		String removedElement = updatedList.remove(indexOfElementToRemove);
		updatedList.add(0, removedElement);
		return StringUtils.join(updatedList,",");
	}

}
