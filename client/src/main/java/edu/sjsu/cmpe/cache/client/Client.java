package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.List;

public class Client {

    public static void main(String[] args) throws Exception {
        
    	System.out.println("Starting Cache Client...");
        
    	CRDTClientService client = new CRDTClientService();
    	List<DistributedCacheService> listOfServers = new ArrayList<DistributedCacheService>();
    	listOfServers.add(new DistributedCacheService("http://localhost:3000"));
    	listOfServers.add(new DistributedCacheService("http://localhost:3001"));
    	listOfServers.add(new DistributedCacheService("http://localhost:3002"));
    	
    	client.addAllServers(listOfServers);
    	
    	System.out.println("Putting values on servers first time....");
    	boolean result = client.put(1, "a");
    	System.out.println("Key->1 and value->a");
    	System.out.println("Operation successful ? -- "+result);
    	
    	Thread.sleep(30000);
    	
    	System.out.println("Putting values on servers second time....");
    	result = client.put(1, "b");
    	System.out.println("Key->1 and value->b");
    	System.out.println("Operation successful ? -- "+result);
    	
    	Thread.sleep(30000);
    	
    	String resultValue = client.get(1);
    	System.out.println("Read successful with value:"+ resultValue);
    	
    	
    	
    }

}
