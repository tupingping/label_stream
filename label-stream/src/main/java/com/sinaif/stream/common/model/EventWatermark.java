package com.sinaif.stream.common.model;

import javax.annotation.Nullable;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;


public class EventWatermark implements AssignerWithPeriodicWatermarks<Event> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Event metricEvent, long previousElementTimestamp) {
        if (metricEvent.timestamp > currentTimestamp) {
            this.currentTimestamp = metricEvent.timestamp;
        }
        return currentTimestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);

    }
}
