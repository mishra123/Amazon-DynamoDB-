/*
 * Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample 
{
	
    public static void main(String[] args) throws Exception 
    {
    	String data;
    	Properties properties = new Properties();
		properties.load(AmazonDynamoDBSample.class
				.getResourceAsStream("/AwsCredentials.properties"));

		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));
		// AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
		System.out.println("Hi");
		// Create an Amazon EC2 Client
		// Create Instance Request
		
    	AmazonDynamoDBClient client = new AmazonDynamoDBClient(bawsc);
    	String file_name = "caltech-256.csv";
    	String data_array[] = new String[30607];
    	File f =new File(file_name);
    	System.out.println("test start");
    	try
    	{
    		Scanner input= new Scanner(f); 
    		for(int i = 0; i < 30607; i++)
    		{
    			data_array[i] = input.next();
    		}
    		for(int j = 0 ; j < data_array.length; j++)
    		{
    			System.out.println(data_array[j]);
    		}
    		for(int k = 1; k < 30607; k++)
    		{
    		String[] single_part = data_array[k].split(",");
    		System.out.println("String : " + single_part[0] + " 2: "+single_part[1] + " 3 : " + single_part[2]);
    		
    		
    		// Uploading data to DynamoDb Table
    		
    		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
             item.put("Category", new AttributeValue(single_part[0]));
             item.put("Picture", new AttributeValue().withN(single_part[1]));
             item.put("S3URL", new AttributeValue(single_part[2]));
             PutItemRequest putItemIntoTable = new PutItemRequest().withTableName("anoop_table1").withItem(item);
             client.putItem(putItemIntoTable);
             
             item.clear();
            
    		
    		}
    		System.out.println("Task Completed");
    	}	
    	catch(Exception ee)
    	{ 
    		System.out.println("Exception is : "  + ee);
    	}
           	
    	        
    	      
}
}