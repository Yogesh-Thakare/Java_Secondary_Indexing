package com.adb.lookup;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.adb.constants.Constants;

/**
 * This class processes the query based on the input provided input can either be specific age or age range
 * 
 *
 */
public class QueryProcessor {
	
	File[] listOfIndexFiles;
	File originalInputFile;
	String outputfileName;
	List<File> tempFileList;
	
	/**
	 * Constructor to initialise resource files
	 */
	public QueryProcessor()
	{
		 originalInputFile = new File(Constants.INPUT_FILE_PATH+"/"+Constants.INPUT_FILE);
		 outputfileName=Constants.OUTPUT_FILE_PATH+"/"+Constants.OUTPUT_FILE;
		 File indexfolder = new File(Constants.INDEX_FILE_PATH);
		 listOfIndexFiles = indexfolder.listFiles(); 
	}
		
	/**
	 * This method fetches the records matching specific age from original file
	 * @param age
	 */
	public void fetchResultsForAge(int age)
	{
		tempFileList= new ArrayList<File>();
		
		try
		{
			if(listOfIndexFiles!=null)
			{
				for(File tempFile: listOfIndexFiles)
				{			
					if (age==(Integer.parseInt((tempFile.getName().substring(0, tempFile.getName().lastIndexOf("."))))))
						tempFileList.add(tempFile);
				}
			
				long startTime = System.currentTimeMillis();
				System.out.println("**---------------------------RESULTS--------------------------------**");
				System.out.println("#PHASE2: Search started for query...");
				long records=processResult(tempFileList, new File(outputfileName),originalInputFile);
				System.out.println("Search finished... see result folder in eclipse workspace");
				long endTime = System.currentTimeMillis();
		
				System.out.println("No of records fetched: "+records);
				System.out.println("Time taken to fetch result is: "+ (endTime-startTime)+"milliseconds");
				calculateAvgSal();
			}
			else
			{
				System.err.println("Failed to fetch records...bucket files not found");
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}		
	}
	
	/**
	 * This method fetches the records matching between age range from original file
	 * @param lowerAge
	 * @param higherAge
	 */
	public void fetchResultsForAge(int lowerAge,int higherAge)
	{
		tempFileList= new ArrayList<File>();
		
		
		try
		{
			for(File tempFile: listOfIndexFiles)
			{			
				if ((Integer.parseInt((tempFile.getName().substring(0, tempFile.getName().lastIndexOf(".")))))>=lowerAge && 
				(Integer.parseInt((tempFile.getName().substring(0, tempFile.getName().lastIndexOf(".")))))<=higherAge)
				tempFileList.add(tempFile);
			}
			
			long startTime = System.currentTimeMillis();
			System.out.println("**---------------------------RESULTS--------------------------------**");
			System.out.println("#PHASE2: Search started for query...");
			long records=processResult(tempFileList, new File(outputfileName),originalInputFile);
			System.out.println("Search finished... see result folder in eclipse workspace");
			long endTime = System.currentTimeMillis();
		
			System.out.println("No of records fetched: "+records);
			System.out.println("Time taken to fetch result is: "+ (endTime-startTime)+"milliseconds");
			calculateAvgSal();
		
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}		
	}
	
	/**
	 * Carry out search process on the basis of bucket files chosen and write results to output file
	 * @param files
	 * @param outputfile
	 * @param originalInputFile
	 * @throws IOException
	 */
	public static long processResult(List<File> files, File outputfile, File originalInputFile) throws IOException 
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));
		RandomAccessFile bucketAccess=null;
		RandomAccessFile inputAccess=null;
		long records=0;
		
		try
		{
			
		for (File f : files) 
		{
			bucketAccess = new RandomAccessFile(f, "r");
			FileChannel inChannel = bucketAccess.getChannel();
			inputAccess= new RandomAccessFile(originalInputFile, "r");
		
			byte[] byteArray = new byte[100];
			long bucketSize = f.length();
			int bytesRead = 0;
			int totalBytesReadCount = 0;
			int indexOffset = 0;
			ByteBuffer indexBlock;
			ByteBuffer indexEntry;
			byte[] indexBytes = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0};
			long currentBlockStart;
			int blockOffset;
			long offset;
			
			
			while (indexOffset < bucketSize) 
			{
				indexBlock = getBlock(inChannel);
				indexOffset += Constants.BLOCK_SIZE;
				currentBlockStart = 0;
				blockOffset = 0;
				
				while (indexBlock.hasRemaining()) 
				{
					
					indexBlock.get(indexBytes, 3, 5);
					indexEntry = ByteBuffer.wrap(indexBytes);
					offset = indexEntry.getLong();

					inputAccess.seek(offset*100);
					bytesRead = inputAccess.read(byteArray, 0, 100);
					totalBytesReadCount += bytesRead;
					
					if(bytesRead != -1)
					{
						bw.write(""+new String(byteArray));
						bw.newLine();
						records++;
					}
					else
					{
						System.out.println("Something is going wrong. time is:"+System.currentTimeMillis() + " and the tuple offset is:" +offset);
						
					}
				}
			}
		}
		}
				
		catch(IOException ex)
		{
			ex.printStackTrace();
		}
		
		finally 
		{ 
			inputAccess.close();
			bw.close();
		}
		
		return records;
	}
		
	/**
	 * @param readChannel
	 * @return
	 * @throws IOException
	 */
	public static ByteBuffer getBlock(FileChannel readChannel) throws IOException 
	{
		ByteBuffer block = ByteBuffer.allocate(Constants.BLOCK_SIZE-1);		
		block.clear();
		readChannel.read(block);
		block.flip();
		return block;
	}
	
	/**
	 * Calculates average salary of all the persons in original file
	 */
	public static void calculateAvgSal()
	{
		RandomAccessFile access;
				
		try {
			long rowCounter = 0;
			byte[] byteArray = new byte[10];
			access = new RandomAccessFile("./input/person.txt", "r");
			int bytesRead = 0;
			String data = "";
			long totalSalary=0;
			
				while(bytesRead != -1) {
					  
					while((bytesRead != -1) )
					{ 
						access.seek((rowCounter)*100+ 41); //17 is location from which we need to start reading
						bytesRead =  access.read(byteArray, 0, 10);
						if(bytesRead != -1){
							//Get the data
							data  = new String(byteArray);
							totalSalary+=Integer.parseInt(data);						
						}
						rowCounter ++;
					}		
					System.out.println("Average salary of "+(rowCounter-1)+" persons is: "+((double)totalSalary/(rowCounter-1)));
					
					access.close();
				} 		
		}
		catch (FileNotFoundException e) {
			System.out.println("Input File not found.");
			e.printStackTrace();
		}
		catch(Exception oef)
		{
			oef.printStackTrace();
		}
		
		
	
}
	
}
