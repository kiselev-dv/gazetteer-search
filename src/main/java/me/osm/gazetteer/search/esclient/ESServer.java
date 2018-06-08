package me.osm.gazetteer.search.esclient;

import java.net.InetAddress;

import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ESServer {
	
	public static final ESServer instance = new ESServer();
	
	public static ESServer getInstance() {
		return instance;
	}

	private TransportClient client;
	
	private ESServer() {
		
		try {
			System.setProperty("es.set.netty.runtime.available.processors", "false");
			
			Settings settings = Settings.builder()
			        .put("cluster.name", "gazetteer")
			        .put("client.transport.ignore_cluster_name", "true").build();
			
			client = new PreBuiltTransportClient(settings);
			
			TransportAddress transportAddress = 
					new TransportAddress(InetAddress.getByName("localhost"), 9300);
			
			client.addTransportAddress(transportAddress);
			
		}
		catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void close() {
		if (client != null) {
			client.close();
		}
	}
	
	public IndicesAdminClient indicesAdminClient() {
		return client.admin().indices();
	}
	
	public TransportClient client() {
		return client;
	}

}
