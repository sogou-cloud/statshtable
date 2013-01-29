/*
Copyright 2012 Urban Airship and Contributors
 */

package com.urbanairship.statshtable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * We have to subclass MetricsRegistry since MetricsRegistry since we want to
 * use SHTimerMetrics instead of normal TimerMetrics. The normal MetricsRegistry
 * doesn't allow this.
 */
public class StatsTimerRegistry {
	private final String scopeSuffix;
	private static final Log log = LogFactory.getLog(StatsTimerRegistry.class);

	private static final LoadingCache<MetricName, Metric> cache = CacheBuilder
			.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
			.removalListener(new RemovalListener<MetricName, Metric>() {

				@Override
				public void onRemoval(
						RemovalNotification<MetricName, Metric> notification) {
					log.info("LoadingCache notify listener to remove name:"
							+ notification.getKey());
					Metrics.defaultRegistry().removeMetric(
							notification.getKey());
				}

			}).build(new CacheLoader<MetricName, Metric>() {

				@Override
				public Timer load(MetricName key) throws Exception {
					log.info("LoadingCache load miss key: " + key);
					return Metrics.newTimer(key, TimeUnit.MILLISECONDS,
							TimeUnit.SECONDS);
				}
			});

	public StatsTimerRegistry(String scopeSuffix) {
		this.scopeSuffix = scopeSuffix;
	}

	/**
	 * Get the SHTimerMetric if one already exists for the given scope&name,
	 * else create one.
	 * 
	 * @throws ExecutionException
	 */
	public Timer newSHTimerMetric(String scope, String name)
			throws ExecutionException {
		if (scope == null) {
			scope = "";
		}
		MetricName metricName = StatsHTable.newMetricName(scope + scopeSuffix,
				name);
		return (Timer) cache.get(metricName);
	}

	public Map<MetricName, Metric> allMetrics() {
		Map<MetricName, Metric> timers = Collections.unmodifiableMap(cache
				.asMap());
		Map<MetricName, Metric> matchTimers = new HashMap<MetricName, Metric>();
		for (Entry<MetricName, Metric> entry : timers.entrySet()) {
			if (entry.getKey().getScope().endsWith(scopeSuffix)) {
				matchTimers.put(entry.getKey(), entry.getValue());
			}
		}
		return matchTimers;
	}

	public void removeMetric(MetricName name) {
		cache.invalidate(name);
	}
}
