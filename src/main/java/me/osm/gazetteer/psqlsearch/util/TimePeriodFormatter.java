package me.osm.gazetteer.psqlsearch.util;

import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

public class TimePeriodFormatter {
	
	private static final PeriodFormatter formatter = new PeriodFormatterBuilder()
			.appendDays()
			.appendSuffix("d")
			.appendSeparator(" ")
			.appendHours()
			.appendSuffix("h")
			.appendSeparator(" ")
			.appendMinutes()
			.appendSuffix("m")
			.appendSeparator(" ")
			.appendSeconds()
			.appendSuffix("s")
			.appendSeparator(" ")
			.appendMillis3Digit()
			.appendSuffix("ms")
			.toFormatter();

	public static String printDuration(long time) {
		Duration duration = new Duration(time);
		
		String durationString = formatter.print(duration.toPeriod());
		return durationString;
	}
}
