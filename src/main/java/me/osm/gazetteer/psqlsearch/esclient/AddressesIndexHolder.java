package me.osm.gazetteer.psqlsearch.esclient;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;

public class AddressesIndexHolder {
	
	public static final String INDEX_NAME = "addresses";
	public static final String ADDR_ROW_TYPE = "addr_row";
	
	private static final JSONObject mapping = readMapping("es_mappings/addr_row.json");

	private static JSONObject readMapping(String path) {
		try {
			
			return new JSONObject(IOUtils.toString(
					AddressesIndexHolder.class.getClassLoader()
						.getResourceAsStream(path)));
			
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	public static boolean exists() {
		
		boolean indexExists = ESServer.getInstance().indicesAdminClient()
	    	.prepareExists(INDEX_NAME).execute().actionGet().isExists();
		
		if (indexExists) {
			return ESServer.getInstance().indicesAdminClient()
				.prepareTypesExists(INDEX_NAME).setTypes(ADDR_ROW_TYPE)
				.execute().actionGet().isExists();
		}
		
		return false;
		
	}
	
	public static void create() {
		IndicesAdminClient indicesAdminClient = ESServer.getInstance().indicesAdminClient();
		indicesAdminClient.prepareCreate(INDEX_NAME).get();
		indicesAdminClient.preparePutMapping(INDEX_NAME)
			.setType(ADDR_ROW_TYPE)
			.setSource(mapping.toString(), XContentType.JSON)
			.get();
	}

	public static void drop() {
		IndicesAdminClient indicesAdminClient = ESServer.getInstance().indicesAdminClient();
		indicesAdminClient.prepareDelete(INDEX_NAME).get();
	}
	
}
