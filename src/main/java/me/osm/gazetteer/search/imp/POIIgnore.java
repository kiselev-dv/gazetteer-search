package me.osm.gazetteer.search.imp;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.osm.gazetteer.search.imp.osmdoc.OSMDoc;
import me.osm.osmdoc.model.Group;
import me.osm.osmdoc.model.Hierarchy;

public class POIIgnore {
	
	private static final Logger log = LoggerFactory.getLogger(POIIgnore.class);

	private Hierarchy defHierarchy;

	private Set<String> named = new HashSet<>();
	private Set<String> keep = new HashSet<>();
	private Set<String> drop = new HashSet<>();
	
	public POIIgnore(OSMDoc doc, JSONObject cfg) {
		JSONObject imp = cfg.getJSONObject("import");
		
		List<Hierarchy> hierarchies = doc.getReader().listHierarchies();
		Optional<Hierarchy> ff = hierarchies.stream()
				.filter(h -> h.getName().equals(imp.optString("default_hierarchy"))).findFirst();
		
		if (ff.isPresent()) {
			defHierarchy = ff.get();
		}
		else if (!hierarchies.isEmpty()) {
			defHierarchy = hierarchies.get(0);
		}
		
		list(imp.optJSONArray("include")).forEach(t -> {
			if(t.named) {
				named.add(t.feature);
			}
			else {
				keep.add(t.feature);
			}
		});
		
		list(imp.optJSONArray("exclude")).forEach(t -> {
			if(t.named) {
				named.add(t.feature);
			}
			else {
				drop.add(t.feature);
			}
		});
		
		log.info("Keep if named {}", named);
		log.info("Drop {}", drop);
		log.info("Keep {}", keep);
	}
	
	public boolean keep(List<String> clazz, boolean hasName) {
		for(String cls : clazz) {
			if (keep(cls, hasName)) {
				return true;
			}
		}
		return false;
	}
	
	public boolean keep(String clazz, boolean hasName) {
		if (keep.contains(clazz)) {
			return true;
		}
		
		if (named.contains(clazz)) {
			return hasName;
		}
		
		if (drop.contains(clazz)) {
			return false;
		}
		
		return true;
	}

	private static final class Tuple {
		String feature;
		boolean named;
		
		public Tuple(String f, boolean named) {
			this.feature = f;
			this.named = named;
		}
	}
	
	private Collection<Tuple> list(JSONArray inc) {
		Set<Tuple> result = new HashSet<>();
		
		if (inc != null) {
			for (int i = 0; i < inc.length(); i++) {
				
				Object incl = inc.get(i);
				
				if (incl instanceof String) {
					result.add(new Tuple((String) incl, false));
				}
				
				if (incl instanceof JSONObject) {
					JSONObject obj = (JSONObject) incl;
					boolean named = obj.optBoolean("keep_named", false);
					
					String type = StringUtils.stripToNull(obj.optString("type"));
					if (type != null) {
						result.add(new Tuple(type, named));
					}
					
					String subtree = StringUtils.stripToNull(obj.optString("subtree"));
					if(subtree != null) {
						Group g = findGroup(defHierarchy.getGroup(), subtree);
						if (g != null) {
							Set<String> features = new HashSet<>();
							collectFeatures(g, obj.optBoolean("ignore_children", false), features);
							
							features.forEach(s -> result.add(new Tuple(s, named)));
						}
					}
				}
			}
		}
		
		return result;
	}

	private void collectFeatures(Group g, boolean noKids, Set<String> result) {
		if(g.getFref() != null) {
			g.getFref().forEach(fref -> result.add(fref.getRef()));
		}
		
		if (g.getGroup() != null && !noKids) {
			for (Group cg : g.getGroup()) {
				collectFeatures(cg, noKids, result);
			}
		}
	}

	private Group findGroup(List<Group> groups, String name) {
		for (Group g : groups) {
			if (g.getName().equals(name)) {
				return g;
			}
			if (g.getGroup() != null) {
				Group dfs = findGroup(g.getGroup(), name);
				if (dfs != null) {
					return dfs;
				}
			}
		}
		return null;
	}

	
}
