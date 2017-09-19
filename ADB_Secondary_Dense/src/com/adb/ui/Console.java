package com.adb.ui;

import java.util.InputMismatchException;
import java.util.Scanner;

import com.adb.Indexing.IndexProcessor;
import com.adb.lookup.QueryProcessor;

/**
 * This is the main class accepts input and display the results on console.
 *
 */
public class Console 
{
	
	public static void main(String[] args)
	{

	    int choice=0;
	    int age;
	    int lowerAge;
	    int higherAge;
	    boolean isSuccess = false;
	    
	    IndexProcessor indexCreator;
	    QueryProcessor query;
	    Scanner ageScanner=null;
	    
	    System.out.println("###------------------- WELCOME TO QUERY CONSOLE -------------------###");
	    
	    try
	    {
	    	indexCreator = new IndexProcessor();
	    	isSuccess=indexCreator.initIndexing();
	    	if(isSuccess)
	    	{
	    		System.out.println("Press 1 for specific age else press 2 for age range");
	    		ageScanner = new Scanner(System.in);
	    		choice = ageScanner.nextInt();
	    	}
	    }
	    catch (InputMismatchException  ex)
	    {
	    	System.err.println("Not a valid integer");
	    }
	    
	    switch (choice) 
	    {
	        case 1: //for specific age
	        	System.out.println("Enter age: ");
	        	try
	        	{
	        		age=ageScanner.nextInt();
	        		        	
	        		if(age<18||age>98)
	        		{
	        			System.err.println("Invalid age...Exiting from program");
	        			break;
	        		}
	        
	        		if(isSuccess)
	        		{
	        			query=new QueryProcessor();
	        			query.fetchResultsForAge(age);
	        			System.out.println("**---------------------------THANK YOU------------------------------**");
	        		} 
	        	}
	        	catch (InputMismatchException  ex)
	        	{
	        		System.err.println("Not a valid integer...Exiting from program");
	        	}
	            break;
	     
	        case 2: // for range of age
	            System.out.println("Enter lower and higher age: ");
	            try
	            {
	            	lowerAge=ageScanner.nextInt();
	            	higherAge=ageScanner.nextInt();
	            	if(lowerAge<18||higherAge>98)
	            	{
	            		System.err.println("Invalid age range...Exiting from program");
	            		break;
	            	}
	            	indexCreator = new IndexProcessor();
	            	isSuccess=indexCreator.initIndexing();
	            	if(isSuccess)
	            	{
	            		query=new QueryProcessor();
	            		query.fetchResultsForAge(lowerAge,higherAge);
	            		System.out.println("**---------------------------THANK YOU----------------------------**");
	            	}
	            }
	            catch (InputMismatchException  ex)
	            {
	            	System.err.println("Not a valid integer...Exiting from program");
	            }
	            break;
	        case 0:
	        	System.err.println("Something went wrong...Exiting from program");
	            break;
	        default:
	        	System.err.println("Invalid option selected...Exiting from program");
	            break;
	        		     
	    }
	    if(ageScanner!=null)
	    ageScanner.close();	   
	}
}
