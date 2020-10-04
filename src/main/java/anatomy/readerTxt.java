package anatomy;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class readerTxt {
	
	public ArrayList<String> Read(String path) {
		
		String fileName = path;
		String tuple = null;
		//String[] words = null;
		ArrayList<String> list = new ArrayList<String>();
 		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
	        while((tuple = bufferedReader.readLine()) != null) {
	        	//words = line.split(" ");
	        	if ( tuple.length() != 0 )
	        		list.add(tuple);	
	        }  
	        bufferedReader.close();
		}
	    catch(FileNotFoundException ex) {
	        System.out.println("Unable to open file '" + fileName + "'");                
	    }
	    catch(IOException ex) {
	        System.out.println("Error reading file '" + fileName + "'");                  
	    }
 		return list;
	}	

	public ArrayList<List<String>> ReadClusters ( String path , int groupId ) {
		
		String fileName = path;
		String tuple = null;
		ArrayList<List<String>> arrayOfClusters = new ArrayList<List<String>>();
		int groupID = 0;
		int currentGroupID = 0;
		boolean didntRead = true;
		try {
			
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			ArrayList<String> cluster = null;
			
	        while((tuple = bufferedReader.readLine()) != null) {
	        	
	        	if ( tuple.length() != 0 ) {
	        		
	        		if( didntRead ) {
		        		
		        		cluster = new ArrayList<String>();
		        		currentGroupID = 0;
		        		didntRead = false;
		        		cluster.add( tuple );
		        		
		        	}else {
		        		
		        		groupID = Integer.parseInt( tuple.split(" ")[groupId].toString() );
			        	if ( groupID == currentGroupID ) {
			        		cluster.add( tuple );
			        	}else {
			        		currentGroupID = groupID;
			        		arrayOfClusters.add( cluster );
			        		cluster = new ArrayList<String>();
			        		cluster.add( tuple );
			        	}	
		        	}
	        	}
	        	
	        } 
	        
	        arrayOfClusters.add( cluster );
    		
	        bufferedReader.close();
		}
		catch(FileNotFoundException ex) {
	        System.out.println("Unable to open file '" + fileName + "'");                
	    }
	    catch(IOException ex) {
	        System.out.println("Error reading file '" + fileName + "'");                  
	    }
		return arrayOfClusters;
	}
}
