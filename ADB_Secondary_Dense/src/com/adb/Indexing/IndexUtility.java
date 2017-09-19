package com.adb.Indexing;

import java.io.FileNotFoundException;
import java.io.IOException;
import com.adb.constants.Constants;
/**
 * Utility to create fileoutputstreams, read from input,write to bucket files.
 * Also computes total DISK I/O made in terms of reads and writes to and from file.
 * 
 *
 */
public class IndexUtility {
	
	private BucketHolder[] BHs;

	/**
	 * creates fileoutputstreams equal to the size of bucket
	 * @throws FileNotFoundException
	 */
	public IndexUtility() throws FileNotFoundException 
	{
		BHs = new BucketHolder[Constants.MAXIMUM_BUCKET_SIZE];
		for (int i = 0; i < BHs.length; ++i) 
		{
			BHs[i] = new BucketHolder(Constants.INDEX_FILE_PATH +"/"+ Integer.toString(i + Constants.LOWER_BUCKET_NUMBER)+".txt");
		}
	}
	
	/**
	 * @param age
	 * @return
	 */
	public BucketHolder getRelativeAge(short age) 
	{
		return BHs[age-Constants.LOWER_BUCKET_NUMBER];
	}
	
	/**
	 * @param age
	 * @param offset
	 */
	public void checkRelativeBucket(short age, long lineNumber) 
	{
		getRelativeAge(age).fillBuffer(lineNumber);
	}

	public void close() throws IOException 
	{
		for(int i = 0; i < BHs.length; ++i) 
		{
			BHs[i].close();
		}
	}
	
	public  BucketHolder[] getBHs() 
	{
		return BHs;
	}

	/**
	 * Computes total read counts required to read blocks of 4096 bytes from input file
	 * @param totalRecords
	 * @param recordsPerBlock
	 * @return
	 */
	public static int getTotalReadCount(long totalRecords, long recordsPerBlock)
	{
		int totalReads;
		totalReads=(int) Math.ceil((double)totalRecords/recordsPerBlock);		
		return totalReads;
	}	
	
	/**
	 * Computes total write counts required to write inside respective bucket files
	 * @param BHs
	 * @return
	 */
	public int getTotalWriteCounts() 
	{
		int sum = 0;
		for(int i = 0; i < BHs.length; ++i) {
			sum += BHs[i].getWrites();
		}
		return sum;
	}
	

}
