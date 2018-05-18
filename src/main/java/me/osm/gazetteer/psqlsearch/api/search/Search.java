package me.osm.gazetteer.psqlsearch.api.search;

import me.osm.gazetteer.psqlsearch.api.ResultsWrapper;

public interface Search {

	ResultsWrapper search(String query, int page, int pageSize, SearchOptions options);

}
