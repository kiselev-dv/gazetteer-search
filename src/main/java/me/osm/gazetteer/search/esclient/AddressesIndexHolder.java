package me.osm.gazetteer.search.esclient;

public class AddressesIndexHolder extends IndexHolder {

	public AddressesIndexHolder() {
		super(IndexHolder.ADDRESSES_INDEX, IndexHolder.ADDR_ROW_TYPE, "es_mappings/addr_row.json");
	}

}
