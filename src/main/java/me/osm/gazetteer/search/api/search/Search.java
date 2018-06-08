package me.osm.gazetteer.search.api.search;

import me.osm.gazetteer.search.api.ResultsWrapper;

public interface Search {

	ResultsWrapper search(String query, int page, int pageSize, SearchOptions options);

}
