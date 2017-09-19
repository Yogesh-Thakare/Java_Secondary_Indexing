package com.adb.Indexing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import com.adb.constants.Constants;

/**
 * This class contains the logic of Index processing
 * 
 *
 */
public class IndexProcessor {
	
	/**
	 * Initiate setup before indexing
	 */
	public boolean initIndexing()
	{
		
		String inputFileName=Constants.INPUT_FILE_PATH+"/"+Constants.INPUT_FILE;
		System.out.println("Scanning for input file in eclipse workspace...");
		System.out.println("");
		
		return(processIndexing(new File(inputFileName), Constants.MAXIMUM_BUCKET_SIZE));
					
	}

	/**
	 * Create secondary dense indexing on the basis of age & line number with help of bucket
	 * @param inputFile
	 * @param maxtmpfiles
	 * @param tmpdirectory
	 */
	private boolean processIndexing(File inputFile, int maxtmpfiles) 
	{	
		IndexUtility utility ;
		RandomAccessFile access;
		boolean isIndexCreated=false;
		
		try
		{
			long beforemem = Runtime.getRuntime().freeMemory();
			long startTime = System.currentTimeMillis();
			access = new RandomAccessFile(inputFile, "r");
			FileChannel inChannel = access.getChannel();
			int recordsPerBlock = Constants.BLOCK_SIZE / Constants.RECORD_SIZE;
			byte[] tuple = new byte[2];
			ByteBuffer block4196 = ByteBuffer.allocate(Constants.BLOCK_SIZE + Constants.RECORD_SIZE);
			long lineNumber = 0;
			
			long fileSize = inputFile.length();
			long recordsInRelation = fileSize / Constants.RECORD_SIZE;
			int totalReads=IndexUtility.getTotalReadCount(recordsInRelation,recordsPerBlock);//this utility calculates the total number
																			             //of read required 	
			utility=new IndexUtility();
			
			for (int i = 0; i <= totalReads; i++) 
			{ 
			
			block4196.put(get4096Bytes(inChannel));
			block4196.flip();
			
				while (block4196.remaining() >= Constants.RECORD_SIZE) 
				{
					block4196.position(block4196.position() + Constants.AGE_OFFSET);
					block4196.get(tuple, 0, 2);
					utility.checkRelativeBucket(getCompactAge(new String(tuple)), lineNumber);
					++lineNumber;
					block4196.position((block4196.position() / Constants.RECORD_SIZE) * Constants.RECORD_SIZE + Constants.RECORD_SIZE);
				}
			
			block4196.compact();
			}
		
			utility.close();
			access.close();
			isIndexCreated=true;
		
			long endTime = System.currentTimeMillis();
			long afteremem = Runtime.getRuntime().totalMemory();
			int writeCount=utility.getTotalWriteCounts();
			long ptrSpace=(fileSize*Constants.Pointer_SIZE)/Constants.RECORD_SIZE;
			long blocksUtilised= ptrSpace/Constants.BLOCK_SIZE+(ptrSpace % Constants.BLOCK_SIZE == 0 ? 0 : 1);
			
		
			
			System.out.println("#PHASE1: Indexing creation started...");
			System.out.println("Total WRITES happened for indexing :"+ writeCount);
			System.out.println("Total READS happened for indexing :"+ totalReads);
			System.out.println("Total DISK I/O for indexing: "+ (writeCount+totalReads));
			System.out.println("Total BLOCKS used for indexing: "+ blocksUtilised);
			System.out.println("Main memory used for indexing: "+ (afteremem-beforemem)/1024+"KB");
			System.out.println("Time taken to create indexing is: "+ (endTime-startTime)+"milliseconds");
			System.out.println("Indexing creation finished");
			System.out.println("");
		
		}
		catch(FileNotFoundException fnf)
		{
			System.err.println("File not exits: "+fnf.getMessage());
			System.out.println("Failed to create indexing");
		}
		catch(IOException ioe)
		{
			System.err.println("W "+ ioe.getMessage());
		}
		return isIndexCreated;
	}
			
	/**
	 * get block of 4096 bytes in a sequence from input file
	 * @param fc
	 * @return
	 * @throws IOException
	 */
	public static ByteBuffer get4096Bytes(FileChannel fc) throws IOException
	{
		ByteBuffer block = ByteBuffer.allocate(Constants.BLOCK_SIZE);
		block.clear();
		fc.read(block);
		block.flip();
		return block;
	}
		
	/**
	 * @param age
	 * @return
	 */
	public static Short getCompactAge(String age)
	{
		return Short.parseShort(age);
	}		
}