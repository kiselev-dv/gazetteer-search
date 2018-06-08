package me.osm.gazetteer.search.api;

import org.restexpress.Request;
import org.restexpress.Response;

public interface SearchAPI {
	
	public ResultsWrapper read(Request request, Response response);

}
