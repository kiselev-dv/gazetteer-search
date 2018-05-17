package me.osm.gazetteer.psqlsearch.backendquery.sql;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import me.osm.gazetteer.psqlsearch.backendquery.AbstractSearchQuery;
import me.osm.gazetteer.psqlsearch.backendquery.StandardSearchQueryRow;
import me.osm.gazetteer.psqlsearch.dao.ConnectionPool;
import me.osm.gazetteer.psqlsearch.imp.postgres.Importer;
import me.osm.gazetteer.psqlsearch.named_jdbc_stmnt.NamedParameterPreparedStatement;

public class SQLSearchQuery extends AbstractSearchQuery {
	
	private static final String template;
	private static final ConnectionPool pool = ConnectionPool.getInstance();
	
	static {
		try {
			template = IOUtils.toString(
					Importer.class.getResourceAsStream("/search/basic.sql"));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private NamedParameterPreparedStatement getStatement() throws SQLException {
		
		NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement
				.createNamedParameterPreparedStatement(pool.getConnection(), template);
		
		setWeights(stmt);
		
		stmt.setString("required_terms_or", TSVector.asTSVectorOR(required, prefix));
		stmt.setString("required_terms_and", TSVector.asTSVectorAND(required, prefix));
		
		stmt.setString("opt_like", getOptLike());
		
//		Array hnExactArray = stmt.getConnection().createArrayOf("text", housenumberExact.toArray());
		Array hnVariantsArray = stmt.getConnection().createArrayOf("text", housenumberVariants.toArray());
		
		String hn_exact = housenumberExact.isEmpty() ? null : housenumberExact.get(0);
		stmt.setString("hn_exact", hn_exact);
		stmt.setArray("hn_var", hnVariantsArray);
		
		applyPaging(stmt);
		
		//System.out.println(stmt.toString());
		
		return stmt;
	}

	private void setWeights(NamedParameterPreparedStatement stmt) throws SQLException {
		stmt.setFloat("full_match_weight", 100.0f);
		
		if (required.size() > 1) {
			stmt.setFloat("street_match_weight", 75.0f);
			stmt.setFloat("locality_match_weight", 50.0f);
		}
		else {
			stmt.setFloat("locality_match_weight", 75.0f);
			stmt.setFloat("street_match_weight", 65.0f);
		}
	}
	
	private String getOptLike() {
		
		if(optional != null && !optional.isEmpty()) {
			for (String t : optional) {
				if(!StringUtils.containsAny(t, "0123456789")) {
					return StringUtils.substring(t, 0, 3).toLowerCase() + "%";
				}
			}
		}
		
		return "NONE";
	}

	public List<StandardSearchQueryRow> listResults() throws Exception {
		List<StandardSearchQueryRow> result = new ArrayList<>(getPageSize());
		
		NamedParameterPreparedStatement stmt = getStatement();
		try {
			ResultSet rs = stmt.executeQuery();
			
			while(rs.next()) {
				StandardSearchQueryRow row = new StandardSearchQueryRow();
				
				row.setRank(rs.getDouble("rank"));
				row.setFullText(rs.getString("full_text"));
				row.setJson(rs.getString("json"));
				row.setOsmId(rs.getString("osm_id"));
				
				result.add(row);
			}
			
			return result;
		}
		finally {
			Connection connection = stmt.getConnection();
			stmt.close();
			connection.close();
		}
	}
	
	private void applyPaging(NamedParameterPreparedStatement stmt) throws SQLException {
		stmt.setInt("limit", pageSize);
		stmt.setInt("offset", (page - 1) * pageSize);
	}
	
}
