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

package com.spotify.bigtable.read;

import com.google.bigtable.v2.Row;
import com.google.common.collect.ImmutableList;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TableReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  TableRead.TableReadImpl tableRead;

  @Before
  public void setUp() throws Exception {
    tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
  }

  @Test
  public void testRow() throws Exception {
    final RowRead.RowReadImpl row = (RowRead.RowReadImpl) tableRead.row("row");
    assertEquals("row", row.readRequest.getRows().getRowKeys(0).toStringUtf8());
    assertEquals(1, row.readRequest.getRows().getRowKeysCount());
    assertEquals(0, row.readRequest.getRows().getRowRangesCount());
    assertEquals(1, row.readRequest.getRowsLimit());
    assertEquals(bigtableMock.getMockedDataClient(), row.getClient());
  }

  @Test
  public void testRows() throws Exception {
    final RowsRead.RowsReadImpl rows = (RowsRead.RowsReadImpl) tableRead.rows();
    assertEquals(tableRead.readRequest().build(), rows.readRequest().build());
    assertEquals(bigtableMock.getMockedDataClient(), rows.getClient());
  }

  @Test
  public void testReadRequest() throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), tableRead.readRequest().getTableName());
  }

  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), tableRead.getClient());
  }

  @Test
  public void testToDataType() throws Exception {
    final List<Row> rows = ImmutableList.of(Row.getDefaultInstance());
    assertEquals(rows, tableRead.toDataType(rows));
  }
}
