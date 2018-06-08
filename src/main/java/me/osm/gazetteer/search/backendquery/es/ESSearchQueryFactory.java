package me.osm.gazetteer.search.backendquery.es;

import me.osm.gazetteer.search.backendquery.AbstractSearchQuery;
import me.osm.gazetteer.search.backendquery.SearchQueryFactory;

public class ESSearchQueryFactory implements SearchQueryFactory {

	@Override
	public AbstractSearchQuery newQuery() {
		return new ESSearchQuery();
	}

}
