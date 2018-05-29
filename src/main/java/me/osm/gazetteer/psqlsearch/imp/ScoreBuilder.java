package me.osm.gazetteer.psqlsearch.imp;

import me.osm.gazetteer.psqlsearch.imp.addr.AddrRowWrapper;

public interface ScoreBuilder {

	double getScore(AddrRowWrapper subj);

}
