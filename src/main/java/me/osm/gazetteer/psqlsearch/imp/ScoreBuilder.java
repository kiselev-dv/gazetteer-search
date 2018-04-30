package me.osm.gazetteer.psqlsearch.imp;

import me.osm.gazetteer.psqlsearch.imp.es.AddrRowWrapper;

public interface ScoreBuilder {

	double getScore(AddrRowWrapper subj);

}
