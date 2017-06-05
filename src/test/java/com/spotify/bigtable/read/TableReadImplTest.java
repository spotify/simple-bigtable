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

package com.spotify.bigtable.read;

import static org.junit.Assert.assertEquals;

import com.google.appengine.repackaged.com.google.common.collect.Sets;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class TableReadImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  TableRead.TableReadImpl tableRead;

  @Before
  public void setUp() throws Exception {
    tableRead = new TableRead.TableReadImpl(bigtableMock, "table");
  }

  private void verifyReadRequest(ReadRowsRequest.Builder readRequest) throws Exception {
    assertEquals(bigtableMock.getFullTableName("table"), readRequest.getTableName());
  }

  @Test
  public void testRow() throws Exception {
    final ReadRow.RowSingleRead.ReadImpl row = tableRead.row("row");
    verifyReadRequest(row.readRequest());
    assertEquals("row", row.readRequest.getRows().getRowKeys(0).toStringUtf8());
    assertEquals(1, row.readRequest.getRows().getRowKeysCount());
    assertEquals(0, row.readRequest.getRows().getRowRangesCount());
    assertEquals(1, row.readRequest.getRowsLimit());
    assertEquals(bigtableMock.getMockedDataClient(), row.getClient());
  }

  @Test
  public void testRows() throws Exception {
    final ReadRows.RowsReadImpl rows = tableRead.rows();
    verifyReadRequest(rows.readRequest());
    assertEquals(RowSet.getDefaultInstance(), rows.readRequest().getRows());
    assertEquals(bigtableMock.getMockedDataClient(), rows.getClient());
  }

  @Test
  public void testRowsCollection() throws Exception {
    final ImmutableSet<String> rowKeys = ImmutableSet.of("row1", "row2");
    final ReadRow.RowMultiRead.ReadImpl rows = tableRead.rows(rowKeys);
    final ReadRowsRequest.Builder readRequest = rows.readRequest();
    verifyReadRequest(readRequest);
    final Set<ByteString> rowKeysBytes =
        rowKeys.stream().map(ByteString::copyFromUtf8).collect(Collectors.toSet());
    assertEquals(rowKeysBytes, Sets.newHashSet(readRequest.getRows().getRowKeysList()));
    assertEquals(2, readRequest.getRows().getRowKeysCount());
    assertEquals(0, readRequest.getRows().getRowRangesCount());
    assertEquals(2, readRequest.getRowsLimit());
    assertEquals(bigtableMock.getMockedDataClient(), rows.getClient());
  }

  @Test
  public void testGetClient() throws Exception {
    assertEquals(bigtableMock.getMockedDataClient(), tableRead.getClient());
  }

  @Test
  public void testToDataType() throws Exception {
    final List<Row> rows = ImmutableList.of(Row.getDefaultInstance());
    assertEquals(rows, tableRead.toDataType().apply(rows));
  }
}
