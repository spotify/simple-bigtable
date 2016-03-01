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

package com.spotify.bigtable;

import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.bigtable.mutate.BigtableMutation;
import com.spotify.bigtable.read.TableRead;
import com.spotify.bigtable.readmodifywrite.BigtableReadModifyWrite;
import com.spotify.bigtable.readmodifywrite.BigtableReadModifyWriteImpl;
import com.spotify.bigtable.sample.BigtableSampleRowKeys;
import com.spotify.bigtable.sample.BigtableSampleRowKeysImpl;

import java.io.IOException;

public class Bigtable {

  private final BigtableSession session;
  private final String project;
  private final String zone;
  private final String cluster;

  public Bigtable(final BigtableSession session, final String project, final String zone, final String cluster) {
    this.session = session;
    this.project = project;
    this.zone = zone;
    this.cluster = cluster;
  }

  public BigtableSession getSession() {
    return session;
  }

  public String getProject() {
    return project;
  }

  public String getZone() {
    return zone;
  }

  public String getCluster() {
    return cluster;
  }

  public void close() throws IOException {
    session.close();
  }

  /**
   * Read some data from a Bigtable table.
   *
   * @param table table name
   * @return TableRead
   */
  public TableRead read(final String table) {
    return new TableRead.TableReadImpl(this, table);
  }

  /**
   * Perform mutations on a row atomically.
   *
   * @param table table name
   * @param row   row key
   * @return BigtableMutation
   */
  public BigtableMutation mutateRow(final String table, final String row) {
    return new BigtableMutation.BigtableMutationImpl(this, table, row);
  }

  /**
   * Perform atomic read following by modify/write operations on the latest value of the
   * specified columns within a row.
   *
   * @param table table name
   * @param row   row key
   * @return BigtableReadModifyWrite
   */
  public BigtableReadModifyWrite readModifyWrite(final String table, final String row) {
    return new BigtableReadModifyWriteImpl(this, table, row);
  }

  /**
   * Sample row keys from a table.
   *
   * @param table table name
   * @return BigtableSampleRowKeys
   */
  public BigtableSampleRowKeys sampleRowKeys(final String table) {
    return new BigtableSampleRowKeysImpl(this, table);
  }
}
