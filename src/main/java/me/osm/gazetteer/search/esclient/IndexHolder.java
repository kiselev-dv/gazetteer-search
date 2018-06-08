package me.osm.gazetteer.search.esclient;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;

public class IndexHolder {
	
	public static final String ADDRESSES_INDEX = "addresses";
	public static final String ADDR_ROW_TYPE = "addr_row";
	
	public static final String POI_CLASS_INDEX = "osmdoc";
	public static final String POI_CLASS_TYPE = "poi_class";

	private String index;
	private String type;
	private String mapping;

	public IndexHolder(String index, String type, String mapping) {
		this.index = index;
		this.type = type;
		this.mapping = mapping;
	}
	
	public String getIndex() {
		return index;
	}
	
	public String getType() {
		return type;
	}

	public void create() {
		IndicesAdminClient indicesAdminClient = ESServer.getInstance().indicesAdminClient();
		
		boolean indexExists = ESServer.getInstance().indicesAdminClient()
    		.prepareExists(index).execute().actionGet().isExists();
		
		if (!indexExists) {
			indicesAdminClient.prepareCreate(index).get();
		}

		indicesAdminClient.preparePutMapping(index)
			.setType(type)
			.setSource(readMapping().toString(), XContentType.JSON)
			.get();
	}

	public void drop() {
		IndicesAdminClient indicesAdminClient = ESServer.getInstance().indicesAdminClient();
		indicesAdminClient.prepareDelete(index).get();
	}
	
	public boolean exists() {
		
		boolean indexExists = ESServer.getInstance().indicesAdminClient()
	    	.prepareExists(index).execute().actionGet().isExists();
		
		if (indexExists) {
			return ESServer.getInstance().indicesAdminClient()
				.prepareTypesExists(index).setTypes(type)
				.execute().actionGet().isExists();
		}
		
		return false;
	}
	
	public JSONObject readMapping() {
		try {
			
			return new JSONObject(IOUtils.toString(
					IndexHolder.class.getClassLoader()
						.getResourceAsStream(mapping)));
			
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
}
