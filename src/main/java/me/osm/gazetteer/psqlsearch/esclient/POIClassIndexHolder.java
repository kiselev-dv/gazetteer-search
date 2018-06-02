package me.osm.gazetteer.psqlsearch.esclient;

public class POIClassIndexHolder extends IndexHolder {

	public POIClassIndexHolder() {
		super(IndexHolder.POI_CLASS_INDEX, IndexHolder.POI_CLASS_TYPE, "es_mappings/poi_class.json");
	}

}

