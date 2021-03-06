package me.osm.gazetteer.search.api.search;

import java.util.ArrayList;
import java.util.List;

import me.osm.gazetteer.search.api.ResultsWrapper;
import me.osm.gazetteer.search.backendquery.AbstractSearchQuery;
import me.osm.gazetteer.search.backendquery.SearchQueryFactory;
import me.osm.gazetteer.search.backendquery.StandardSearchQueryRow;
import me.osm.gazetteer.search.backendquery.es.ESSearchQueryFactory;
import me.osm.gazetteer.search.query.QToken;
import me.osm.gazetteer.search.query.Query;
import me.osm.gazetteer.search.query.QueryAnalyzer;
import me.osm.gazetteer.search.query.QueryAnalyzerImpl;

/**
 * @Deprecated see ESDefaultSearch
 * */
public class DeafultSearch implements Search {
	
	private QueryAnalyzer analyzer = new QueryAnalyzerImpl();
	private SearchQueryFactory queryFactory = new ESSearchQueryFactory();

	
	@Override
	public ResultsWrapper search(String queryString, int page, int pageSize, SearchOptions options) {

		Query query = analyzer.getQuery(queryString);
		List<QToken> tokens = query.listToken();
		
		List<String> required = new ArrayList<>();
		List<String> optional = new ArrayList<>();
		List<String> numeric = new ArrayList<>();

		for(QToken token : tokens) {
			if (token.isHasNumbers()) {
				numeric.add(token.toString());
			}
			
			if (token.isOptional()) {
				optional.add(token.toString());
			}
			else if (!token.isNumbersOnly()) {
				required.add(token.toString());
			}
		}
		
		AbstractSearchQuery standardSearchQuery = queryFactory.newQuery();
		
		standardSearchQuery.setHousenumberExact(numeric);
		standardSearchQuery.setHousenumberVariants(numeric);
		standardSearchQuery.setRequired(required);
		standardSearchQuery.setOptional(optional);
		
		standardSearchQuery.setPrefix(options.isWithPrefix());
		
		standardSearchQuery.setPage(page);
		standardSearchQuery.setPageSize(pageSize);
		
		ResultsWrapper result = new ResultsWrapper();
		
		result.setPage(page);
		result.setPageSize(pageSize);
		result.setQuery(query.print());
		
		try {
			List<StandardSearchQueryRow> results = standardSearchQuery.listResults();
			for (StandardSearchQueryRow row : results) {
//				result.addResultsRow(row.getRank(), 0.0, row.getFullText(), 
//						null, row.getOsmId(), row.getOsmId(), null, null, null, null, null);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			result.setErrorMessage(e.getMessage());
		}

		return result;
	}

	public static void main(String[] args) {
		DeafultSearch instance = new DeafultSearch();
		
		for (String q : args) {
			ResultsWrapper result = instance.search(q, 0, 20, new SearchOptions());
			System.out.println(result);
		}
	}
}
