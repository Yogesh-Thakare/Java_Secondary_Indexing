package com.adb.constants;



/**
 * This class contains all constants use inside program.
 * 
 *
 */
public class Constants {
	
		public Constants() 
		{
		}
		public static final String INPUT_FILE_PATH = "./input";
		public static final String INDEX_FILE_PATH = "./bucket";
		public static final String OUTPUT_FILE_PATH = "./result";
		public static final String INPUT_FILE = "person.txt";
		public static final String OUTPUT_FILE = "result.dat";
		public static final int MAXIMUM_BUCKET_SIZE = 81;
		public static final int LOWER_BUCKET_NUMBER = 18;
		public static final int BLOCK_SIZE = 4096;
		public static final int RECORD_SIZE = 100;
		public static final int AGE_OFFSET = 39;
		public static final int Pointer_SIZE = 5;
		
		public static final int BUCKET_POINTER_CAPACITY = BLOCK_SIZE / Pointer_SIZE;	
}
