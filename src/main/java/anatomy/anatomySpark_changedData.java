package anatomy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class anatomySpark_changedData {
	
	public static int sensitiveAttrIndex = 3;
	public static int l = 2;
	
	static class arrayComparator implements Serializable, Comparator<List<String>>{

		private static final long serialVersionUID = 3440667782652419219L;

		public int compare(List<String> s1, List<String> s2) {
			return s2.size() - s1.size(); 
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		ArrayList<List<String>> previousClusters = new ArrayList<List<String>>();
		ArrayList<List<String>> currentClusters = new ArrayList<List<String>>();

		
		ArrayList<Integer> modifiedClusterIndexes = new ArrayList<Integer>();
		ArrayList<Integer> nonSatisfiedClusterIndexes = new ArrayList<Integer>();
		List<String> dataset = new ArrayList<String>();
		
		SparkConf conf = new SparkConf().setAppName("anatomySpark").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		readerTxt reader = new readerTxt();

//		anatomyCreateClusters(sparkContext);
		
		JavaRDD<String> oldData = sparkContext.textFile("testData");
		JavaRDD<String> newData = sparkContext.textFile("testData2");
		
		previousClusters = reader.ReadClusters( "Clusters", sensitiveAttrIndex + 1);
		
//		oldData.collect().forEach(System.out::println);
//		newData.collect().forEach(System.out::println);
		
		JavaRDD<String> sameData = oldData.intersection(newData);
		JavaRDD<String> deletions = oldData.subtract(sameData);
		JavaRDD<String> insertions = newData.subtract(sameData);
		
//		insertions.collect().forEach(System.out::println);	
//		deletions.collect().forEach(System.out::println);
		
		if ( deletions.count() > 0 && insertions.count() == 0 ) {
			
			modifiedClusterIndexes = modifyDeletion( previousClusters, deletions );
			nonSatisfiedClusterIndexes = checkCondition( previousClusters, l, modifiedClusterIndexes );
			if( nonSatisfiedClusterIndexes.size() > 0 ) {
				dataset = createTuples( nonSatisfiedClusterIndexes, previousClusters );
			}
			
			ArrayList<List<String>> newClusters = anatomyCreateClusters( sparkContext, dataset );
			
			currentClusters = mergeClusters ( previousClusters, newClusters ); 
			
		}
		if ( deletions.count() > 0 && insertions.count() > 0 ) {
			
			modifiedClusterIndexes = modifyDeletion( previousClusters, deletions );
			nonSatisfiedClusterIndexes = checkCondition(previousClusters, l, modifiedClusterIndexes);
			if( nonSatisfiedClusterIndexes.size() > 0 ) {
				dataset = createTuples( nonSatisfiedClusterIndexes, previousClusters );
			}
			insertions.collect().forEach( dataset::add );
			
			ArrayList<List<String>> newClusters = anatomyCreateClusters( sparkContext, dataset );
			currentClusters = mergeClusters ( previousClusters, newClusters );
			
		}
		if ( deletions.count() == 0 && insertions.count() > 0 ) {
			
			if ( insertions.collect().size() >= l ) {
				
				insertions.collect().forEach( dataset::add );
				ArrayList<List<String>> newClusters = anatomyCreateClusters( sparkContext, dataset );
				currentClusters = mergeClusters ( previousClusters, newClusters );
				
			}else {
				
				List<String> insertedTuples = insertions.collect();
				Random random = new Random();
				for ( String tuple : insertedTuples ) {
					int index = random.nextInt(previousClusters.size());
					List<String> cluster = previousClusters.get( index );
					tuple += " " + index;
					cluster.add( tuple );
				}
				currentClusters = previousClusters;
			}
		}	
		for ( List<String> cluster : currentClusters ) {		
			for ( String temp1 : cluster ) {
				System.out.println( temp1 );
			}
			System.out.println( "------" );
		}
		
//		population(currentClusters, sparkContext);
		
		sparkContext.close();
		
	}
	private static ArrayList<List<String>> mergeClusters( ArrayList<List<String>> previousClusters,  ArrayList<List<String>> newClusters) {
		
		deleteEmptyClusters( previousClusters );
		ArrayList<List<String>> currentClusters = new ArrayList<List<String>>();
		
		for ( int i = 0; i < previousClusters.size(); i++ ) {
			List<String> cluster = previousClusters.get( i );
			List<String> tempCluster = new ArrayList<>();
			for ( String tuple : cluster ) {
				
				String[] attributes = tuple.split(" ");
				String Tuple = createQidString( attributes, sensitiveAttrIndex + 1 );
				Tuple += " " + ( currentClusters.size() + 1 );
				tempCluster.add( Tuple );
			}
			
			currentClusters.add( tempCluster );
		}
		
		for ( int i = 0; i < newClusters.size(); i++ ) {
			List<String> cluster = newClusters.get( i );
			List<String> tempCluster = new ArrayList<>();
			for ( String tuple : cluster ) {
				
				String[] attributes = tuple.split(" ");
				String Tuple = createQidString( attributes, sensitiveAttrIndex + 1 );
				Tuple += " " + ( currentClusters.size() + 1 );
				tempCluster.add( Tuple );
			}
			
			currentClusters.add( tempCluster );
		}
		
		return currentClusters;
		
	}
	private static void deleteEmptyClusters( ArrayList<List<String>> clusters ) {
		for ( int i = 0; i < clusters.size(); i++ ) {
			List<String> cluster = clusters.get( i );
			
			if ( cluster.size() == 0 ) {
				clusters.remove(cluster);
				i--;
			}
			
		}
	}
	private static List<String> createTuples( ArrayList<Integer> nonSatisfiedClusterIndexes, ArrayList<List<String>> previousClusters ) {
		
		List<String> dataset = new ArrayList<String>();
		
		for ( int i = 0; i < nonSatisfiedClusterIndexes.size(); i++ ) { 
			
			int index = nonSatisfiedClusterIndexes.get( i );
			
			List<String> cluster = previousClusters.get( index );
			
			if ( cluster.size() > 0 ) {
				for ( int j = 0; j < cluster.size(); j++ ) {
					
					String tuple = cluster.get( j );
			
					String[] attributes = tuple.split(" ");
					String Tuple = createQidString( attributes, sensitiveAttrIndex + 1 );
					dataset.add( Tuple ); 
					cluster.remove( tuple );
					
				}
			}
		}
		
		return dataset;
	}
	public static ArrayList<List<String>> anatomyCreateClusters( JavaSparkContext sparkContext , String filename ) throws IOException {
		
		JavaRDD<String> inputData = sparkContext.textFile("testData");
		
		ArrayList<List<String>> arrayOfBuckets = null;
		ArrayList<List<String>> arrayOfClusters = null;
		
		JavaPairRDD<String,String> pairRDD = inputData.mapToPair(new PairFunction<String,String,String>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
			
				String[] value = s.split(" ");
				String returnVal = "";
				for ( int i = 0; i < value.length; i++ ) {
					returnVal += value[i];
					if ( i + 1 != value.length )
						returnVal += " ";
				}
				
				return new Tuple2<String,String>(value[sensitiveAttrIndex],returnVal);
		}});	
		
		List<String> senstiveValuesDistinct = pairRDD.keys().distinct().collect();  
		
		arrayOfBuckets = bucketCreation( inputData, senstiveValuesDistinct );
	
		arrayOfClusters = groupCreation( arrayOfBuckets, l );
		
		residueAssignment( arrayOfBuckets, arrayOfClusters );
		
		writer( arrayOfClusters );
		
		return arrayOfClusters;
		
	}
	public static ArrayList<List<String>> anatomyCreateClusters( JavaSparkContext sparkContext , List<String> dataset ) throws IOException {
		
		JavaRDD<String> inputData = sparkContext.parallelize( dataset );
		
		ArrayList<List<String>> arrayOfBuckets = null;
		ArrayList<List<String>> arrayOfClusters = null;
		
		JavaPairRDD<String,String> pairRDD = inputData.mapToPair(new PairFunction<String,String,String>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
			
				String[] value = s.split(" ");
				String returnVal = "";
				for ( int i = 0; i < value.length; i++ ) {
					returnVal += value[i];
					if ( i + 1 != value.length )
						returnVal += " ";
				}
				
				return new Tuple2<String,String>(value[sensitiveAttrIndex],returnVal);
		}});	
		
		List<String> senstiveValuesDistinct = pairRDD.keys().distinct().collect();  
		
		arrayOfBuckets = bucketCreation( inputData, senstiveValuesDistinct );
	
		arrayOfClusters = groupCreation( arrayOfBuckets, l );
		
		residueAssignment( arrayOfBuckets, arrayOfClusters );
		
//		for ( List<String> cluster : arrayOfClusters ) {		
//			for ( String temp1 : cluster ) {
//				System.out.println( temp1 );
//			}
//			System.out.println( "------" );
//		}
		
		//writer( arrayOfClusters );
		
		return arrayOfClusters;
		
	}
	public static List<String> mergeClusters ( ArrayList<List<String>> arrayOfClusters ) {
		
		List<String> tuples = new ArrayList<>(); 
		
		for ( List<String> cluster : arrayOfClusters ) {		
			tuples.addAll( cluster );
		}
		
		return tuples;
	}

	public static ArrayList<Integer> modifyDeletion ( ArrayList<List<String>> previousClusters, JavaRDD<String> deletions) {
		
		List<String> deletedTuples = deletions.collect();
		ArrayList<Integer> modifiedClusterIndexes =  new ArrayList<Integer>();
		int index = 0;
		
		for ( String tuple : deletedTuples ) {
			
			index = findCluster( tuple, previousClusters );
//			System.out.println( index );
			List<String> cluster = previousClusters.get( index );
			
			String[] attributes = tuple.split(" ");
			String sensitiveAttr = attributes[sensitiveAttrIndex]; 
			String QIDs = createQidString( attributes, sensitiveAttrIndex + 1 );
			
			for ( int i = 0; i < cluster.size(); i++ ) {
				
				String temp = cluster.get( i );
				
				String[] tempAttributes = temp.split(" ");
				String tempQIDs = createQidString( tempAttributes, sensitiveAttrIndex + 1 );
				
				if ( tempQIDs.equals( QIDs ) ) {
					try {
						
						cluster.remove( temp );
						
						if ( !modifiedClusterIndexes.contains( index ))
							modifiedClusterIndexes.add( index );
						break;
						
					}catch( Exception ex ) {
						System.out.println( ex.getMessage().toString() );
						return null;
					}
				}
			}
		}
		return modifiedClusterIndexes;
		
	}

	public static ArrayList<Integer> checkCondition( ArrayList<List<String>> arrayOfCurrentClusters, int l, ArrayList<Integer> modifiedClusterIndexes ) {
		
		ArrayList<Integer> nonSatisfiedClusterIndexes = new ArrayList<Integer>();
		
		for ( int i = 0; i < modifiedClusterIndexes.size(); i++ ) {
			
			int index = modifiedClusterIndexes.get( i );
			
			List<String> cluster = arrayOfCurrentClusters.get( index );
			
			if ( cluster.size() < l ) {
				nonSatisfiedClusterIndexes.add( index );
			}else {
				if ( checkLCondition( cluster, l ) == false ) {
					nonSatisfiedClusterIndexes.add( index );
				}
			}
		}
		return nonSatisfiedClusterIndexes;
	}
	
	public static boolean checkLCondition( List<String> cluster, int l ) {
		
		ArrayList<String> sensitiveAttributes = new ArrayList<String>();
		
		for ( int i = 0; i < cluster.size(); i++ ) {
			String temp = cluster.get( i );
			sensitiveAttributes.add( temp.split(" ")[sensitiveAttrIndex].toString() );
		}
	
		sensitiveAttributes = distinctString( sensitiveAttributes );
		
		if ( sensitiveAttributes.size() >= l ) {
			return true;
		}else {
			return false;
		}
		
	}
	public static ArrayList<String> distinctString ( ArrayList<String> array ) {
		
		ArrayList<String> returnArray = new ArrayList<String>();
		
		for ( String value : array ) {
			if( !returnArray.contains(value) ) {
				returnArray.add( value );
			}
		}
		return returnArray;
	}
	
	public static int findCluster( String tuple, ArrayList<List<String>> arrayOfCurrentClusters ) {
		
		String[] attributes = tuple.split(" ");
		String QIDs = createQidString(attributes, sensitiveAttrIndex + 1);
		
		for ( List<String> cluster : arrayOfCurrentClusters ) {
			for ( String tempTuple : cluster ) {
				
				String[] tempAttributes = tempTuple.split(" ");
				String tempQIDs = createQidString(tempAttributes, sensitiveAttrIndex + 1);
				if ( tempQIDs.equals( QIDs) ) {
					
					int index = arrayOfCurrentClusters.indexOf( cluster );
					return index;
				}
			}
		}
		return 0;
	}
	
	public static ArrayList<List<String>> bucketCreation( JavaRDD<String> datas, List<String> arrayOfSAs ){
		
		ArrayList<List<String>> arrayOfBuckets = new ArrayList<>();
		
		for( String value: arrayOfSAs ) {
			
			ArrayList<String> list = new ArrayList<>();
			
			JavaRDD<String> bucket = datas.filter( new Function<String, Boolean>() {
				
				private static final long serialVersionUID = 1L;

				public Boolean call(String x) { 
					String[] attrs = x.split(" ");
					
					return attrs[sensitiveAttrIndex].equals(value);
				}
			});
			
			bucket.collect().forEach( list::add );
			
			arrayOfBuckets.add( list );
		}
		return arrayOfBuckets;
	}
	
	public static ArrayList<List<String>> groupCreation ( ArrayList<List<String>> arrayOfBuckets, int l ) {
		
		int i, j = 0;
		int groupID = 1;
		
		ArrayList<List<String>> arrayOfClusters = new ArrayList<>();

		while ( arrayOfBuckets.size() >= l ) { 
		
			arrayOfBuckets.sort(new arrayComparator());
			
			j = l;
			
			ArrayList<String> cluster = new ArrayList<>();
			
			for ( i = 0; i < j; i++ ) {
				
				List<String> array = arrayOfBuckets.get( i );
				
				String tuple = array.get( 0 ); 
				
				tuple += " " + groupID;
				
				cluster.add( tuple );
				
				array.remove( 0 );
				
				if ( array.size() == 0 ) {
					
					arrayOfBuckets.remove( i );
					i--;
					j--;
					
				}
			}
				
			arrayOfClusters.add( cluster );
			groupID ++;
		}
		
		return arrayOfClusters;
	
	}
	
	public static void residueAssignment ( ArrayList<List<String>> arrayOfBuckets, ArrayList<List<String>> arrayOfClusters ) {
		
		boolean condition = false;
		int j = arrayOfBuckets.size();
		
		if ( j > 0 ){
			
			for ( int i = 0; i < j; i++ ) {
				
				List<String> array = arrayOfBuckets.get( i );
				
				String tuple = array.get( 0 );
								
				ArrayList<List<String>> subSet = new ArrayList<>();
				
				for ( List<String> temp : arrayOfClusters ) {
					
					condition = false;						 
					for ( String temp1 : temp ) {
						if ( temp1.contains( tuple )) {
							condition = true;
						}
					}
					if ( !condition ) {
						subSet.add( temp );
					}
				}
				
				if ( subSet.size() > 0 ) {
					
					Random rand = new Random(); 
					int index = rand.nextInt( subSet.size() ); 
					int groupID = index + 1;
					List<String> addedArray = subSet.get( index );
					tuple += " " + groupID;
					addedArray.add( tuple );
					arrayOfBuckets.remove( i );
					i--;
					j--;
					
				}
			}
		}
	}
	public static void population( ArrayList<List<String>> arrayOfClusters , JavaSparkContext sparkContext) {
		
		List<String> tuples = mergeClusters( arrayOfClusters );
		
		JavaRDD<String> rddTuples = sparkContext.parallelize( tuples );
		
		JavaRDD<String> QIT = rddTuples.map( new Function<String, String>() { 
	
			private static final long serialVersionUID = 1L;

			public String call(String s) {
				String[] attr = s.split(" ");
				return attr[0] + " " + attr[1] + " " + attr[2] + " " + attr[4];
		    }
		});
		
		JavaPairRDD<String,Integer> pairKeys = rddTuples.mapToPair(new PairFunction<String,String,Integer>(){

			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
			
				String[] attr = s.split(" ");
				String key = attr[4] + " " + attr[sensitiveAttrIndex];
				
				return new Tuple2<String,Integer>( key, 1 );
		}});
		
	   JavaPairRDD<String, Integer> counts = pairKeys.reduceByKey( new Function2< Integer, Integer, Integer>() {
	
		private static final long serialVersionUID = 1L;

		public Integer call(Integer a, Integer b) {
                return a + b;
           }
        });
	   
	   counts = counts.sortByKey();
		
	   QIT.saveAsTextFile("QIT/");
	   counts.saveAsTextFile("ST/");
	}
	public static void writer ( ArrayList<List<String>> arrayOfClusters ) throws IOException {
		
		String fileName = "Clusters1";
		try {
		
			FileWriter fileWriter = new FileWriter(fileName);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			
//			for ( ArrayList<String> temp : arrayOfClusters ) {
//				for ( String tuple : temp ) {
//					
//					bufferedWriter.write( tuple );	
//					bufferedWriter.newLine();
//				}
//			}
			
			for ( int i = 0; i < arrayOfClusters.size(); i++ ) {
				List<String> temp = arrayOfClusters.get( i );
				for ( String tuple : temp ) {
					String[] attributes = tuple.split(" ");
					String returnValue = createQidString(attributes, sensitiveAttrIndex + 1) +  " " + i;
					bufferedWriter.write( returnValue );	
					bufferedWriter.newLine();
				}
			}
			
			bufferedWriter.close();
			
		}catch(IOException ex) {
		    System.out.println("Error writing to file '");
		}
		catch(Exception ex){
			System.out.println( ex.getMessage().toString() );
		}
		
	}
	public static String createQidString ( String[] attributes, int attributeRange ) {
		String returnValue = "";
		
		for ( int i = 0; i < attributeRange; i++ ) { 
			returnValue += attributes[i];
			if ( i + 1 != attributeRange)
				returnValue += " ";
		}
		
		return returnValue;
	}
}
