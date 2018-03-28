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

import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.read.ReadColumn.ColumnWithinRowsRead;
import com.spotify.bigtable.read.ReadFamilies.FamiliesWithinRowsRead;
import com.spotify.bigtable.read.ReadFamily.FamilyWithinRowsRead;
import com.spotify.bigtable.read.ReadRow.RowRead;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RowsRead is an interface for reading a set of rows.
 */
public class ReadRows {

  public interface RowsRead extends RowRead<
      FamilyWithinRowsRead, FamiliesWithinRowsRead, ColumnWithinRowsRead, List<Row>> {

    /**
     * Add a collection of String keys to the read.  BigTable stores keys as an byte[]. This method
     * will incur a conversion of each String key to ByteString key.  Use
     * {@link #addKeysBinary(Collection)} instead.
     * @param rowKeys collection of String keys
     * @return a RowsRead
     */
    RowsRead addKeys(final Collection<String> rowKeys);

    /**
     * Add a collection of Binary keys to the read.
     * @param rowKeys collection of ByteString keys
     * @return a RowsRead
     */
    RowsRead addKeysBinary(final Collection<ByteString> rowKeys);

    RowsRead limit(final long limit);

    RowsRead addRowRangeOpen(final ByteString startKeyOpen, final ByteString endKeyOpen);

    RowsRead addRowRangeClosed(final ByteString startKeyClosed, final ByteString endKeyClosed);

    ResultScanner<Row> execute();

  }

  static class RowsReadImpl extends AbstractBigtableRead<List<Row>, List<Row>> implements RowsRead {

    public RowsReadImpl(final TableRead.TableReadImpl tableRead) {
      super(tableRead);
      readRequest.setRows(RowSet.getDefaultInstance());
    }

    @Override
    public RowsReadImpl addKeys(Collection<String> rowKeys) {
      final Set<ByteString> iterator =
          rowKeys.stream().map(ByteString::copyFromUtf8).collect(Collectors.toSet());
      addKeysBinary(iterator);
      return this;
    }

    @Override
    public RowsReadImpl addKeysBinary(Collection<ByteString> rowKeys) {
      readRequest.setRows(readRequest.getRowsBuilder().addAllRowKeys(rowKeys));
      return this;
    }

    @Override
    public RowsReadImpl limit(final long limit) {
      readRequest.setRowsLimit(limit);
      return this;
    }

    @Override
    public RowsReadImpl addRowRangeOpen(final ByteString startKeyOpen,
                                        final ByteString endKeyOpen) {
      final RowRange.Builder rowRange =
          RowRange.newBuilder().setStartKeyOpen(startKeyOpen).setEndKeyOpen(endKeyOpen);
      readRequest.setRows(readRequest.getRowsBuilder().addRowRanges(rowRange));
      return this;
    }

    @Override
    public RowsReadImpl addRowRangeClosed(final ByteString startKeyClosed,
                                          final ByteString endKeyClosed) {
      final RowRange.Builder rowRange =
          RowRange.newBuilder().setStartKeyClosed(startKeyClosed).setEndKeyClosed(endKeyClosed);
      readRequest.setRows(readRequest.getRowsBuilder().addRowRanges(rowRange));
      return this;
    }

    @Override
    public ResultScanner<Row> execute() {
      return getClient().readRows(readRequest().build());
    }

    @Override
    public FamilyWithinRowsRead family(final String family) {
      return new FamilyWithinRowsRead.ReadImpl(this).columnFamily(family);
    }

    @Override
    public FamiliesWithinRowsRead familyRegex(final String familyRegex) {
      return new FamiliesWithinRowsRead.ReadImpl(this).familyRegex(familyRegex);
    }

    @Override
    public FamiliesWithinRowsRead families(final Collection<String> families) {
      return familyRegex(toExactMatchAnyRegex(families));
    }

    @Override
    public ColumnWithinRowsRead column(final String column) {
      final List<String> parts = Arrays.asList(column.split(":", 2));
      return family(parts.get(0)).columnQualifier(parts.get(1));
    }

    @Override
    protected Function<List<Row>, List<Row>> parentTypeToCurrentType() {
      return Function.identity();
    }
  }
}
