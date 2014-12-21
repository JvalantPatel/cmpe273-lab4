package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

/*
 * Created by Jvalant on 12/20/2014.
 */

public class CRDTClientService {

	private List<DistributedCacheService> serverList;

	public void addAllServers(List<DistributedCacheService> serverList) {
		this.serverList = new ArrayList<DistributedCacheService>();
		this.serverList.addAll(serverList);
	}

	/** asynchronous put method for all servers **/

	public boolean put(long key, String value) {
		final AtomicInteger putCounter = new AtomicInteger(0);
		final List<DistributedCacheService> serverListWithValue = new ArrayList<DistributedCacheService>();
		final AtomicInteger checkServerForSuccess = new AtomicInteger(
				serverList.size());

		for (final DistributedCacheService server : serverList) {
			Future<HttpResponse<JsonNode>> future = Unirest
					.put(server.getServerUrl() + "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {
						public void failed(UnirestException e) {
							System.out.println("Write operation failure for "
									+ server.getServerUrl());
							checkServerForSuccess.decrementAndGet();
						}

						public void completed(HttpResponse<JsonNode> response) {
							serverListWithValue.add(server);
							System.out.println("Write operation done for "
									+ server.getServerUrl());
							putCounter.incrementAndGet();
							checkServerForSuccess.decrementAndGet();
						}

						public void cancelled() {
							System.out.println("Write operation cancelled for "
									+ server.getServerUrl());
							checkServerForSuccess.decrementAndGet();
						}
					});
		}

		while (true) {
			if (checkServerForSuccess.get() == 0)
				break;
		}

		if (putCounter.get() > (serverList.size() / 2)) {
			System.out.println("put counter -- " + putCounter);
			// System.out.println("Put operation successful for "+serverListWithValue.size()+" nodes out of "+serverList.size());
			System.out.println("Put operation done !!");
			return true;
		}

		else {
			System.out.println("Put failed .. deleting values from servers");
			for (DistributedCacheService server : serverListWithValue) {
				server.delete(key);
				System.out.println("Deleted value from node: "
						+ server.getServerUrl());
			}
			return false;
		}

	}

	/** get and repair operation **/

	public String get(long key) {

		final HashMap<DistributedCacheService, String> serverValuesMap = new HashMap<DistributedCacheService, String>();
		final AtomicInteger asyncCount = new AtomicInteger(serverList.size());

		for (final DistributedCacheService server : serverList) {

			Future<HttpResponse<JsonNode>> future = Unirest
					.get(server.getServerUrl() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {
						public void failed(UnirestException e) {
							System.out.println("Read operation failed for "
									+ server.getServerUrl());
							asyncCount.decrementAndGet();
						}

						public void completed(HttpResponse<JsonNode> response) {
							if (response.getCode() == 200)
								serverValuesMap.put(server, response.getBody()
										.getObject().getString("value"));
							else
								serverValuesMap.put(server, "");
							System.out.println("Read operation successful for "
									+ server.getServerUrl());
							asyncCount.decrementAndGet();
						}

						public void cancelled() {
							System.out.println("Read operation canceled for "
									+ server.getServerUrl());
							asyncCount.decrementAndGet();
						}
					});
		}

		while (true) {

			if (asyncCount.get() == 0)
				break;

		}

		HashMap<DistributedCacheService, String> repairedMap = getMapAfterRepair(serverValuesMap);

		if (repairedMap.size() == 0) {
			System.out.println("No repair operation required !!");
			List<String> readValues = new ArrayList<String>(serverValuesMap.values());
			return readValues.get(0);
		} else {

			System.out.println("Repair operation Required !!");
			
			List<String> readValues = new ArrayList<String>(repairedMap.values());
			for (Entry<DistributedCacheService, String> entry : repairedMap.entrySet()) {
				final DistributedCacheService server = entry.getKey();
				String value = entry.getValue();
				System.out.println("Repairing: " + server.getServerUrl());
				Future<HttpResponse<JsonNode>> future = Unirest
						.put(server.getServerUrl()+ "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJsonAsync(new Callback<JsonNode>() {
							public void failed(UnirestException e) {
								System.out.println("The repair to "+ server.getServerUrl()+ " has failed");
							}

							public void completed(HttpResponse<JsonNode> response) {
								System.out.println("The repair to "+ server.getServerUrl()+ " was successful");
							}

							public void cancelled() {
								System.out.println("The repair to "+ server.getServerUrl()+ " has been cancelled");
							}
						});
			}
			return readValues.get(0);

		}

	}

	private HashMap<DistributedCacheService, String> getMapAfterRepair(
			HashMap<DistributedCacheService, String> serverValuesMap) {

		HashMap<DistributedCacheService, String> repairedMap = new HashMap<DistributedCacheService, String>();
		List<String> values =new ArrayList<String>(serverValuesMap.values());
		String valueToUpdate = "";
		Iterator iterator = serverValuesMap.entrySet().iterator();
		int maxValue = (int) (Math.floor((serverList.size() / 2)) + 1);

		for (String temp : values) {
			if (Collections.frequency(values, temp) >= maxValue) {
				valueToUpdate = temp;
				break;
			}
		}

		while (iterator.hasNext()) {
			Map.Entry pair = (Map.Entry) iterator.next();
			if (!pair.getValue().equals(valueToUpdate))
				repairedMap.put((DistributedCacheService) pair.getKey(),
						valueToUpdate);
		}

		return repairedMap;
	}
}
