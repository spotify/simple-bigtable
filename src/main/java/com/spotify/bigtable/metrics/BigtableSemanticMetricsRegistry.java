/*
 *
 *  * Copyright 2016 Spotify AB.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.spotify.bigtable.metrics;

import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

import com.google.cloud.bigtable.metrics.Counter;
import com.google.cloud.bigtable.metrics.Meter;
import com.google.cloud.bigtable.metrics.MetricRegistry;
import com.google.cloud.bigtable.metrics.Timer;

/**
 * Metrics registry using Spotify's semantic metrics.
 */
public class BigtableSemanticMetricsRegistry implements MetricRegistry {

  private final SemanticMetricRegistry registry;
  private final MetricId baseMetricId;

  /**
   * Constructor.
   *
   * @param registry Semantic metrics registry
   * @param baseMetricId Base metric Id
   */
  public BigtableSemanticMetricsRegistry(final SemanticMetricRegistry registry,
                                         final MetricId baseMetricId) {
    this.registry = registry;
    this.baseMetricId = baseMetricId;
  }

  /**
   * Constructor.
   *
   * @param registry Semantic metrics registry
   */
  public BigtableSemanticMetricsRegistry(final SemanticMetricRegistry registry) {
    this(registry, MetricId.EMPTY);
  }

  @Override
  public Counter counter(final String name) {
    final MetricId tagged = baseMetricId.tagged("what", name);
    final com.codahale.metrics.Counter counter = registry.counter(tagged);
    return new Counter() {
      @Override
      public void inc() {
        counter.inc();
      }

      @Override
      public void dec() {
        counter.dec();
      }
    };
  }

  @Override
  public Timer timer(final String name) {
    final MetricId tagged = baseMetricId.tagged("what", name);
    final com.codahale.metrics.Timer timer = registry.timer(tagged);
    return new Timer() {
      @Override
      public Context time() {
        final com.codahale.metrics.Timer.Context context = timer.time();
        return context::close;
      }
    };
  }

  @Override
  public Meter meter(final String name) {
    final MetricId tagged = baseMetricId.tagged("what", name);
    final com.codahale.metrics.Meter meter = registry.meter(tagged);
    return new Meter() {
      @Override
      public void mark() {
        meter.mark();
      }

      @Override
      public void mark(long size) {
        meter.mark(size);
      }
    };
  }
}
