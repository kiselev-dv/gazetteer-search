package me.osm.gazetteer.search.imp.addr;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class NamesAccessor {
	
	private JSONObject jsonObject;
	private JSONObject addrObject;
	
	private Collection<String> languages;
	private JSONObject optTags;
	private JSONObject optRefs;
	private Map<String, String> refToLevel;
	private Map<String, JSONObject> namesByLevel;

	private Collection<String> fullNameTags;
	private Collection<String> shortNameTags;

	public NamesAccessor(JSONObject jsonObject, Collection<String> languages) {
		this.jsonObject = jsonObject;
		this.addrObject = jsonObject.optJSONObject("address");
		this.languages = languages;
		
		this.optTags = jsonObject.optJSONObject("tags");
		this.optRefs = jsonObject.optJSONObject("refs");
		
		fullNameTags = fillNameTags(Arrays.asList("name", "old_name", "alt_name"));
		shortNameTags = fillNameTags(Arrays.asList("name"));
		
		this.refToLevel = mapRefToLevel();
		this.namesByLevel = listNamesByLevel();
	}

	private Collection<String> fillNameTags(Collection<String> names) {
		Collection<String> tags = new LinkedHashSet<>();
		
		tags.addAll(names);
		tags.add("int_name");
		
		if(languages != null) {
			for(String prefix: names) {
				for(String suffix : languages) {
					tags.add(prefix + ":" + suffix);
				}
			}
		}
		
		return tags;
	}

	public String getAddrFullText() {
		return addrObject != null ? addrObject.optString("longText") : null;
	}

	public void trimNames() {
		Map<String, JSONObject> id2name = listNamesById();
		id2name.values().forEach(names -> {
			HashSet<String> keys = new HashSet<>(names.keySet());
			keys.removeAll(fullNameTags);
			keys.forEach(k -> names.remove(k));
		});
		JSONObject admin0 = listNamesByLevel().get("admin0");
		if (admin0 != null) {
			admin0.remove("old_name");
		}
	}

	public String getAdmin0() {
		return getByLevel("admin0", shortNameTags);
	}

	public String getAdmin1() {
		return getByLevel("admin1", shortNameTags);
	}

	public String getAdmin2() {
		return getByLevel("admin2", shortNameTags);
	}
	
	public String getLocalAdminName() {
		return getByLevel("local_admin", shortNameTags);
	}

	public String getLocality() {
		return getByLevel("locality", fullNameTags);
	}

	public String getStreet() {
		return getByLevel("street", fullNameTags);
	}

	private String getByLevel(String level, Collection<String> tags) {
		String optName = StringUtils.stripToEmpty(jsonObject.optString(level + "_name"));
		JSONObject byLevel = namesByLevel.get(level);
		if (byLevel != null) {
			for(String tag : tags) {
				optName += " " + StringUtils.stripToEmpty(byLevel.optString(tag));
			}
		}
		
		return StringUtils.stripToNull(optName);
	}

	public String getName() {
		return optTags != null ? optTags.optString("name") : null;
	}
	
	public String getAltNames() {
		Map<String, String> names = getNamesMap();
		names.remove("name");

		return StringUtils.join(names.values(), ' ');
	}
	
	private Map<String, String> getNamesMap() {
		if (optTags != null) {
			Map<String, String> names = new HashMap<>();
			
			for (Iterator<String> iterator = optTags.keys(); iterator.hasNext();) {
				String key = iterator.next();
				if (fullNameTags.contains(key)) {
					names.put(key, optTags.getString(key));
				}
			}
			
			return names;
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, String> mapRefToLevel() {
		Map<String, String> map = new HashMap<>();
		if (optRefs != null) {
			optRefs.toMap().entrySet().forEach(entry -> {
				String lvl = entry.getKey();
				Object value = entry.getValue();
				if (value instanceof String) {
					map.put((String) value, lvl);
				} 
				if(value instanceof Collection) {
					for(String sval : ((Collection<String>)value)) {
						map.put(sval, lvl);
					}
				}
			});
		}
		
		return map;
	}
	
	private Map<String, JSONObject> listNamesByLevel() {
		Map<String, JSONObject> namesByLevel = new HashMap<>();
		
		for(Entry<String, JSONObject> entry : listNamesById().entrySet()) {
			String level = refToLevel.get(entry.getKey());
			if(level != null) {
				namesByLevel.put(level, entry.getValue());
			}
		}
		
		return namesByLevel;
	}

	private Map<String, JSONObject> listNamesById() {
		Map<String, JSONObject> parts = listPartsById(addrObject);
		
		Map<String, JSONObject> map = new HashMap<>();
		if (parts != null) {
			parts.entrySet().forEach(entry -> {
				JSONObject names = entry.getValue().optJSONObject("names");
				if (names != null) {
					map.put(entry.getKey(), names);
				}
			});
			return map;
		}
		
		return map;
	}

	private Map<String, JSONObject> listPartsById(JSONObject addrObject) {
		if (addrObject != null) {
			Map<String, JSONObject> map = new HashMap<>();
			JSONArray parts = addrObject.optJSONArray("parts");
			if (parts != null) {
				parts.forEach(o -> {
					JSONObject json = (JSONObject) o;
					map.put(json.optString("lnk"), json); 
				});
				
				return map;
			}
		}
		return null;
	}

	public boolean isEmpty() {
		return getName() == null && getAddrFullText() == null;
	}

}
