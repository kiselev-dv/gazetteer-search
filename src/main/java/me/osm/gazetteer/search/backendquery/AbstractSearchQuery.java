package me.osm.gazetteer.search.backendquery;

import java.util.List;

public abstract class AbstractSearchQuery {
	
	public abstract List<StandardSearchQueryRow> listResults() throws Exception;

	protected List<String> housenumberExact;
	protected List<String> housenumberVariants;
	protected List<String> required;
	protected List<String> optional;
	
	protected int page;
	protected int pageSize;
	protected boolean prefix;

	public AbstractSearchQuery() {
		super();
	}

	public List<String> getRequired() {
		return required;
	}

	public void setRequired(List<String> required) {
		this.required = required;
	}

	public List<String> getHousenumberExact() {
		return housenumberExact;
	}

	public void setHousenumberExact(List<String> housenumberExact) {
		this.housenumberExact = housenumberExact;
	}

	public List<String> getHousenumberVariants() {
		return housenumberVariants;
	}

	public void setHousenumberVariants(List<String> housenumberVariants) {
		this.housenumberVariants = housenumberVariants;
	}

	public List<String> getOptional() {
		return optional;
	}

	public void setOptional(List<String> optional) {
		this.optional = optional;
	}

	public int getPage() {
		return page;
	}

	public void setPage(int page) {
		this.page = page;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public void setPrefix(boolean prefix) {
		this.prefix = prefix;
	}

}