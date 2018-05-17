package me.osm.gazetteer.psqlsearch.api.search;

import java.util.ArrayList;
import java.util.List;

import me.osm.gazetteer.psqlsearch.api.ResultsWrapper;
import me.osm.gazetteer.psqlsearch.backendquery.AbstractSearchQuery;
import me.osm.gazetteer.psqlsearch.backendquery.SearchQueryFactory;
import me.osm.gazetteer.psqlsearch.backendquery.StandardSearchQueryRow;
import me.osm.gazetteer.psqlsearch.backendquery.es.ESSearchQueryFactory;
import me.osm.gazetteer.psqlsearch.query.QToken;
import me.osm.gazetteer.psqlsearch.query.Query;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzer;
import me.osm.gazetteer.psqlsearch.query.QueryAnalyzerImpl;

public class DeafultSearch implements Search {
	
	private QueryAnalyzer analyzer = new QueryAnalyzerImpl();
	private SearchQueryFactory queryFactory = new ESSearchQueryFactory();

	
	@Override
	public ResultsWrapper search(String queryString, boolean prefix, 
				Double lon, Double lat, boolean addressesOnly, int page,
			int pageSize) {

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
		
		standardSearchQuery.setPrefix(prefix);
		
		standardSearchQuery.setPage(page);
		standardSearchQuery.setPageSize(pageSize);
		
		ResultsWrapper result = new ResultsWrapper();
		
		result.setPage(page);
		result.setPageSize(pageSize);
		result.setQuery(query.print());
		
		try {
			List<StandardSearchQueryRow> results = standardSearchQuery.listResults();
			for (StandardSearchQueryRow row : results) {
				result.addResultsRow(row.getRank(), 0.0, row.getFullText(), row.getOsmId(), null);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			result.setErrorMessage(e.getMessage());
		}

		return result;
	}


	@Override
	public ResultsWrapper search(String queryString, double[] bbox, String[] poiTypes, int page, int pageSize) {
		return null;
	}

	public static void main(String[] args) {
		DeafultSearch instance = new DeafultSearch();
		
		for (String q : args) {
			ResultsWrapper result = instance.search(q, false, null, null, false, 0, 20);
			System.out.println(result);
		}
	}
}
