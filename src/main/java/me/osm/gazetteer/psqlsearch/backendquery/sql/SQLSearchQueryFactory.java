package me.osm.gazetteer.psqlsearch.backendquery.sql;

import me.osm.gazetteer.psqlsearch.backendquery.AbstractSearchQuery;
import me.osm.gazetteer.psqlsearch.backendquery.SearchQueryFactory;

public class SQLSearchQueryFactory implements SearchQueryFactory {

	@Override
	public AbstractSearchQuery newQuery() {
		return new SQLSearchQuery();
	}

}
