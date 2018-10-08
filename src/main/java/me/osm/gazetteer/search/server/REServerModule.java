package me.osm.gazetteer.search.server;

import com.google.inject.Binder;
import com.google.inject.Module;

import me.osm.gazetteer.search.api.SearchAPI;
import me.osm.gazetteer.search.api.SearchAPIAdapter;
import me.osm.gazetteer.search.api.search.ESDefaultSearch;
import me.osm.gazetteer.search.api.search.Search;

public class REServerModule implements Module {

	@Override
	public void configure(Binder binder) {
		
		binder.bind(SearchAPI.class).to(SearchAPIAdapter.class);
		binder.bind(Search.class).to(ESDefaultSearch.class);
	}

}
