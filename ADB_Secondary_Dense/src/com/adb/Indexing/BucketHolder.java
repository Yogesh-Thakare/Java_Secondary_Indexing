package com.adb.Indexing;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.adb.constants.Constants;

/**
 * Use for holding bucket pointers in memory once buffer reaches its defined threshold 
 * or till end of file detected in input file.
 * If either of the above condition is met then data from the buffer is being written to bucket file.
 * 
 *
 */
public class BucketHolder 
{
	private short size = 0;
	private ByteBuffer holderBuffer;
	private ByteBuffer tempBuffer;
	private FileOutputStream outStream;
	private FileChannel outChannel;
	private int writesCount;
	

	/**
	 * Constructor to initialise buffer
	 * @param filename
	 * @throws FileNotFoundException
	 */
	public BucketHolder(String filename) throws FileNotFoundException 
	{
		outStream = new FileOutputStream(filename);
		outChannel = outStream.getChannel();
		holderBuffer = ByteBuffer.allocateDirect(Constants.BLOCK_SIZE);
		tempBuffer = ByteBuffer.allocateDirect(8);
	}
	
	/**
	 * temporary holding bucket pointers till the capacity not reached to threshold
	 * @param offset
	 */
	public void fillBuffer(long offset)
	{
		tempBuffer.putLong(offset);
		tempBuffer.position(3);
		holderBuffer.put(tempBuffer);
		
		tempBuffer.clear();
		++size;
		if (size == Constants.BUCKET_POINTER_CAPACITY) {
			flushBuffer();
		}
	}
	
	/**
	 * Flush the buffer data to bucket file once capacity reached to threshold
	 */
	private void flushBuffer()
	{
		try {
			holderBuffer.flip();
			outChannel.write(holderBuffer);
			holderBuffer.clear();
			size = 0;
			++writesCount;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * close fileoutputstream after writing buffer data to bucket file
	 * @throws IOException
	 */
	public void close() throws IOException 
	{
		holderBuffer.flip();
		outChannel.write(holderBuffer);
		outStream.close();
		++writesCount;
	}
	
	/**
	 * Return number of times written to index file
	 */
	public int getWrites() 
	{
		return writesCount;
	}
	
}
