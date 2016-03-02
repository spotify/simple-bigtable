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
import com.spotify.bigtable.mutate.BigtableMutationImpl;
import com.spotify.bigtable.read.TableRead;
import com.spotify.bigtable.readmodifywrite.BigtableReadModifyWriteImpl;
import com.spotify.bigtable.sample.BigtableSampleRowKeysImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class BigtableTest {

  @Mock
  BigtableSession bigtableSession;

  Bigtable bigtable;

  @Before
  public void setUp() throws Exception {
    bigtable = new Bigtable(bigtableSession, "project", "zone", "cluster");
  }

  @Test
  public void testGetSession() throws Exception {
    assertEquals(bigtableSession, bigtable.getSession());
  }

  @Test
  public void testGetProject() throws Exception {
    assertEquals("project", bigtable.getProject());
  }

  @Test
  public void testGetZone() throws Exception {
    assertEquals("zone", bigtable.getZone());
  }

  @Test
  public void testGetCluster() throws Exception {
    assertEquals("cluster", bigtable.getCluster());
  }

  @Test
  public void testClose() throws Exception {
    bigtable.close();
    Mockito.verify(bigtableSession).close();
  }

  @Test
  public void testRead() throws Exception {
    final TableRead.TableReadImpl table = (TableRead.TableReadImpl) bigtable.read("table");


    assertEquals(bigtable, table.bigtable);
    assertEquals("table", table.table);
  }

  @Test
  public void testMutateRow() throws Exception {
    final BigtableMutationImpl mutation =
            (BigtableMutationImpl) bigtable.mutateRow("table", "row");

    assertEquals(bigtable, mutation.bigtable);
    assertEquals("table", mutation.table);
    assertEquals("row",  mutation.getMutateRowRequest().getRowKey().toStringUtf8());
  }

  @Test
  public void testReadModifyWrite() throws Exception {
    final BigtableReadModifyWriteImpl readModifyWrite =
            (BigtableReadModifyWriteImpl) bigtable.readModifyWrite("table", "row");

    assertEquals(bigtable, readModifyWrite.bigtable);
    assertEquals("table", readModifyWrite.table);
    assertEquals("row",  readModifyWrite.getReadModifyWriteRequest().getRowKey().toStringUtf8());
  }

  @Test
  public void testSampleRowKeys() throws Exception {
    final BigtableSampleRowKeysImpl sampleRowKeys =
            (BigtableSampleRowKeysImpl) bigtable.sampleRowKeys("table");

    assertEquals(bigtable, sampleRowKeys.bigtable);
    assertEquals("table", sampleRowKeys.table);
  }
}