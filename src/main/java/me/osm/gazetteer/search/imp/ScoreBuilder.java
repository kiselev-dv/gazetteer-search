package me.osm.gazetteer.search.imp;

import me.osm.gazetteer.search.imp.addr.AddrRowWrapper;

public interface ScoreBuilder {

	double getScore(AddrRowWrapper subj);

}
