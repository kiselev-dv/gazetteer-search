package me.osm.gazetteer.search.api.meta;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

public class EndpointMeta {

	private String url;
	private String name;
	private Map<String, String> parameters = new LinkedHashMap<>();

	public EndpointMeta(String url, String name, Class<?> annotatedClass) {
		this.url = url;
		this.name = name;
		
		for (Field f : annotatedClass.getFields()) {
			try {
				QueryParameter param = f.getAnnotation(QueryParameter.class);
				if (param != null) {
					String paramName = (String) f.get(null);
					String description = param.description();
					
					parameters.put(paramName, description);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
