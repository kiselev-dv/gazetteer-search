package me.osm.gazetteer.psqlsearch.backendquery.es;

import me.osm.gazetteer.psqlsearch.backendquery.AbstractSearchQuery;
import me.osm.gazetteer.psqlsearch.backendquery.SearchQueryFactory;

public class ESSearchQueryFactory implements SearchQueryFactory {

	@Override
	public AbstractSearchQuery newQuery() {
		return new ESSearchQuery();
	}

}
