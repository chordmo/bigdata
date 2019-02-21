package com.mo.bigdata.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.ClientConfiguration;

import com.mo.bigdata.model.Address;

public class IgniteRun {
	public static void run() {
		ClientConfiguration cfg = new ClientConfiguration().setAddresses("node1:10800", "node2:10800", "node3:10800");

		try (IgniteClient igniteClient = Ignition.startClient(cfg)) {
			System.out.println();
			System.out.println(">>> Thin client put-get example started.");

			final String CACHE_NAME = "put-get-example";

			ClientCache<Object, Object> cache = igniteClient.getOrCreateCache(CACHE_NAME);

			System.out.format(">>> Created cache [%s].\n", CACHE_NAME);

			Integer key = 1;
			Address val = new Address("1545 Jackson Street", 94612);

			cache.put(key, val);

			System.out.format(">>> Saved [%s] in the cache.\n", val);

			Address cachedVal = (Address) cache.get(key);

			System.out.format(">>> Loaded [%s] from the cache.\n", cachedVal);
		} catch (ClientException e) {
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.err.format("Unexpected failure: %s\n", e);
		}
	}

	public static void get() {
//		ClientConfiguration cfg = new ClientConfiguration().setAddresses("node1:10800");
//		ClientConfiguration cfg = new ClientConfiguration().setAddresses("node1:10800", "node2:10800", "node3:10800");
		ClientConfiguration cfg = new ClientConfiguration().setAddresses("node3:10800");
		try (IgniteClient igniteClient = Ignition.startClient(cfg)) {
			System.out.println();
			System.out.println(">>> Thin client put-get example started.");

			final String CACHE_NAME = "put-get-example";

			ClientCache<Object, Object> cache = igniteClient.getOrCreateCache(CACHE_NAME);

			System.out.format(">>> Created cache [%s].\n", CACHE_NAME);

			Integer key = 1;

			Address cachedVal = (Address) cache.get(key);

			System.out.format(">>> Loaded [%s] from the cache.\n", cachedVal);
		} catch (ClientException e) {
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.err.format("Unexpected failure: %s\n", e);
		}
	}

	public static void main(String[] args) throws IgniteException {
		run();
		get();
	}

}
