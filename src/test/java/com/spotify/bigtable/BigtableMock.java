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

import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import org.mockito.Mockito;

public class BigtableMock extends Bigtable {

  public BigtableMock(final BigtableSession session, final String project, final String zone, final String cluster) {
    super(session, project, zone, cluster);
  }

  public BigtableDataClient getMockedDataClient() {
    return this.getSession().getDataClient();
  }

  public String getFullTableName(final String table) {
    return String.format("projects/%s/zones/%s/clusters/%s/tables/%s",
            getProject(),
            getZone(),
            getCluster(),
            table
    );
  }

  public static BigtableMock getMock() {
    final BigtableDataClient dataClient = Mockito.mock(BigtableDataClient.class);
    final BigtableTableAdminClient tableAdminClient = Mockito.mock(BigtableTableAdminClient.class);
    final BigtableSession session = Mockito.mock(BigtableSession.class);

    Mockito.when(session.getDataClient()).thenReturn(dataClient);
    Mockito.when(session.getTableAdminClient()).thenReturn(tableAdminClient);

    return new BigtableMock(session, "project", "zone", "cluster");
  }

  public static Bigtable getMock(final String project, final String zone, final String cluster) {
    final BigtableDataClient dataClient = Mockito.mock(BigtableDataClient.class);
    final BigtableTableAdminClient tableAdminClient = Mockito.mock(BigtableTableAdminClient.class);
    final BigtableSession session = Mockito.mock(BigtableSession.class);

    Mockito.when(session.getDataClient()).thenReturn(dataClient);
    Mockito.when(session.getTableAdminClient()).thenReturn(tableAdminClient);

    return new BigtableMock(session, project, zone, cluster);
  }
}
