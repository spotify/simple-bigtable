/*-
 * -\-\-
 * simple-bigtable
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

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

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import org.mockito.Mockito;

import java.io.IOException;

public class BigtableMock extends Bigtable {

  public static final String PROJECT_ID = "project";
  public static final String INSTANCE_ID = "instance";

  public BigtableMock(final BigtableSession session, final String project, final String instance) {
    super(session, project, instance);
  }

  public BigtableDataClient getMockedDataClient() {
    return this.getSession().getDataClient();
  }

  public String getFullTableName(final String table) {
    return new BigtableInstanceName(PROJECT_ID, INSTANCE_ID).toTableNameStr(table);
  }

  public static BigtableMock getMock() {
    final BigtableDataClient dataClient = Mockito.mock(BigtableDataClient.class);
    final BigtableTableAdminClient tableAdminClient = Mockito.mock(BigtableTableAdminClient.class);
    final BigtableSession session = Mockito.mock(BigtableSession.class);
    final BigtableOptions options = Mockito.mock(BigtableOptions.class);

    try {
      Mockito.when(session.getDataClient()).thenReturn(dataClient);
      Mockito.when(session.getTableAdminClient()).thenReturn(tableAdminClient);
      Mockito.when(session.getOptions()).thenReturn(options);
      Mockito.when(options.getInstanceName()).thenReturn(new BigtableInstanceName(PROJECT_ID, INSTANCE_ID));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new BigtableMock(session, PROJECT_ID, INSTANCE_ID);
  }

  public static Bigtable getMock(final String project,
                                 final String instance) {
    final BigtableDataClient dataClient = Mockito.mock(BigtableDataClient.class);
    final BigtableTableAdminClient tableAdminClient = Mockito.mock(BigtableTableAdminClient.class);
    final BigtableSession session = Mockito.mock(BigtableSession.class);

    try {
      Mockito.when(session.getDataClient()).thenReturn(dataClient);
      Mockito.when(session.getTableAdminClient()).thenReturn(tableAdminClient);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new BigtableMock(session, project, instance);
  }
}
