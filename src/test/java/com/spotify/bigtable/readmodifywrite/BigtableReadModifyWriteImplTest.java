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

package com.spotify.bigtable.readmodifywrite;

import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.BigtableMock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class BigtableReadModifyWriteImplTest {

  BigtableMock bigtableMock = BigtableMock.getMock();
  BigtableReadModifyWriteImpl bigtableReadModifyWrite;

  @Before
  public void setUp() throws Exception {
    bigtableReadModifyWrite = new BigtableReadModifyWriteImpl(bigtableMock, "table", "row");
  }

  private void verifyReadModifyWriteRequest() throws Exception {
    final ReadModifyWriteRowRequest.Builder readModifyWriteRequest = bigtableReadModifyWrite.getReadModifyWriteRequest();
    assertEquals(bigtableMock.getFullTableName("table"), readModifyWriteRequest.getTableName());
    assertEquals("row", readModifyWriteRequest.getRowKey().toStringUtf8());
  }

  @Test
  public void testExecute() throws Exception {
    verifyReadModifyWriteRequest();
    bigtableReadModifyWrite.execute();
    verify(bigtableMock.getMockedDataClient())
            .readModifyWriteRow(bigtableReadModifyWrite.getReadModifyWriteRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testExecuteAsync() throws Exception {
    verifyReadModifyWriteRequest();
    bigtableReadModifyWrite.executeAsync();
    verify(bigtableMock.getMockedDataClient())
            .readModifyWriteRowAsync(bigtableReadModifyWrite.getReadModifyWriteRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testReadColumn() throws Exception {
    final BigtableReadModifyWriteImpl read = (BigtableReadModifyWriteImpl) bigtableReadModifyWrite.read("family:qualifier");
    verifyReadModifyWriteRequest();

    assertEquals("family", read.getReadModifyWriteRule().getFamilyName());
    assertEquals("qualifier", read.getReadModifyWriteRule().getColumnQualifier().toStringUtf8());
    assertEquals(ByteString.EMPTY, read.getReadModifyWriteRule().getAppendValue());
    assertEquals(0L, read.getReadModifyWriteRule().getIncrementAmount());
    assertEquals(0, read.getReadModifyWriteRequest().getRulesCount());
  }

  @Test
  public void testReadColumnFamilyAndQualifier() throws Exception {
    final BigtableReadModifyWriteImpl read = (BigtableReadModifyWriteImpl) bigtableReadModifyWrite.read("family", "qualifier");
    verifyReadModifyWriteRequest();

    assertEquals("family", read.getReadModifyWriteRule().getFamilyName());
    assertEquals("qualifier", read.getReadModifyWriteRule().getColumnQualifier().toStringUtf8());
    assertEquals(ByteString.EMPTY, read.getReadModifyWriteRule().getAppendValue());
    assertEquals(0L, read.getReadModifyWriteRule().getIncrementAmount());
    assertEquals(0, read.getReadModifyWriteRequest().getRulesCount());
  }

  @Test
  public void testReadThenAppendValue() throws Exception {
    final BigtableReadModifyWriteImpl appendValue = (BigtableReadModifyWriteImpl) bigtableReadModifyWrite
            .read("family:qualifier")
            .thenAppendValue(ByteString.copyFromUtf8("value"));
    verifyReadModifyWriteRequest();

    appendValue.execute();
    appendValue.executeAsync();

    assertEquals(1, appendValue.getReadModifyWriteRequest().getRulesCount());
    assertEquals(ReadModifyWriteRule.getDefaultInstance(), appendValue.getReadModifyWriteRule().build());

    final ReadModifyWriteRule rule = appendValue.getReadModifyWriteRequest().getRules(0);
    assertEquals("family", rule.getFamilyName());
    assertEquals("qualifier", rule.getColumnQualifier().toStringUtf8());
    assertEquals("value", rule.getAppendValue().toStringUtf8());
    assertEquals(0L, rule.getIncrementAmount());

    verify(bigtableMock.getMockedDataClient()).readModifyWriteRow(appendValue.getReadModifyWriteRequest().build());
    verify(bigtableMock.getMockedDataClient()).readModifyWriteRowAsync(appendValue.getReadModifyWriteRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testReadThenIncrement() throws Exception {
    final BigtableReadModifyWriteImpl appendValue = (BigtableReadModifyWriteImpl) bigtableReadModifyWrite
            .read("family:qualifier")
            .thenIncrementAmount(15);
    verifyReadModifyWriteRequest();

    appendValue.execute();
    appendValue.executeAsync();

    assertEquals(1, appendValue.getReadModifyWriteRequest().getRulesCount());
    assertEquals(ReadModifyWriteRule.getDefaultInstance(), appendValue.getReadModifyWriteRule().build());

    final ReadModifyWriteRule rule = appendValue.getReadModifyWriteRequest().getRules(0);
    assertEquals("family", rule.getFamilyName());
    assertEquals("qualifier", rule.getColumnQualifier().toStringUtf8());
    assertEquals(ByteString.EMPTY, rule.getAppendValue());
    assertEquals(15L, rule.getIncrementAmount());

    verify(bigtableMock.getMockedDataClient()).readModifyWriteRow(appendValue.getReadModifyWriteRequest().build());
    verify(bigtableMock.getMockedDataClient()).readModifyWriteRowAsync(appendValue.getReadModifyWriteRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }

  @Test
  public void testMultipleRules() throws Exception {
    final BigtableReadModifyWriteImpl appendValue = (BigtableReadModifyWriteImpl) bigtableReadModifyWrite
            .read("family1:qualifier1").thenIncrementAmount(15)
            .read("family2:qualifier2").thenAppendValue(ByteString.copyFromUtf8("value"))
            .read("family3:qualifier3").thenIncrementAmount(30);
    verifyReadModifyWriteRequest();

    appendValue.execute();
    appendValue.executeAsync();

    assertEquals(3, appendValue.getReadModifyWriteRequest().getRulesCount());
    assertEquals(ReadModifyWriteRule.getDefaultInstance(), appendValue.getReadModifyWriteRule().build());

    ReadModifyWriteRule rule = appendValue.getReadModifyWriteRequest().getRules(0);
    assertEquals("family1", rule.getFamilyName());
    assertEquals("qualifier1", rule.getColumnQualifier().toStringUtf8());
    assertEquals(ByteString.EMPTY, rule.getAppendValue());
    assertEquals(15L, rule.getIncrementAmount());

    rule = appendValue.getReadModifyWriteRequest().getRules(1);
    assertEquals("family2", rule.getFamilyName());
    assertEquals("qualifier2", rule.getColumnQualifier().toStringUtf8());
    assertEquals("value", rule.getAppendValue().toStringUtf8());
    assertEquals(0L, rule.getIncrementAmount());

    rule = appendValue.getReadModifyWriteRequest().getRules(2);
    assertEquals("family3", rule.getFamilyName());
    assertEquals("qualifier3", rule.getColumnQualifier().toStringUtf8());
    assertEquals(ByteString.EMPTY, rule.getAppendValue());
    assertEquals(30L, rule.getIncrementAmount());

    verify(bigtableMock.getMockedDataClient()).readModifyWriteRow(appendValue.getReadModifyWriteRequest().build());
    verify(bigtableMock.getMockedDataClient()).readModifyWriteRowAsync(appendValue.getReadModifyWriteRequest().build());
    verifyNoMoreInteractions(bigtableMock.getMockedDataClient());
  }
}
