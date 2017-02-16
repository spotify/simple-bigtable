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

import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.read.ReadCell.CellWithinColumnsRead;
import com.spotify.bigtable.read.ReadCell.CellWithinFamiliesRead;
import com.spotify.bigtable.read.ReadCell.CellWithinRowsRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinColumnsRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinFamiliesRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinRowsRead;
import com.spotify.bigtable.read.ReadColumn.ColumnRead;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ReadColumns {

  interface ColumnsRead<MultiCol, MultiCell, OneCell, R> extends ColumnRead<MultiCell, OneCell, R> {

    MultiCol startQualifierClosed(final ByteString startQualifierClosed);

    MultiCol startQualifierOpen(final ByteString startQualifierOpen);

    MultiCol endQualifierClosed(final ByteString endQualifierClosed);

    MultiCol endQualifierOpen(final ByteString endQualifierOpen);
  }

  public interface ColumnsWithinFamilyRead extends ColumnsRead<
      ColumnsWithinFamilyRead, CellsWithinColumnsRead, CellWithinColumnsRead, List<Column>> {

    class ReadImpl extends AbstractColumnsRead<
        ColumnsWithinFamilyRead, CellsWithinColumnsRead, CellWithinColumnsRead, List<Column>, Optional<Family>>
        implements ColumnsWithinFamilyRead {

      ReadImpl(final Internal<Optional<Family>> row) {
        super(row);
      }

      @Override
      ColumnsWithinFamilyRead multiCol() {
        return this;
      }

      @Override
      public CellWithinColumnsRead latestCell() {
        return new CellWithinColumnsRead.ReadImpl(this);
      }

      @Override
      public CellsWithinColumnsRead cells() {
        return (CellsWithinColumnsRead) new CellsWithinColumnsRead.ReadImpl(this);
      }

      @Override
      protected Function<Optional<Family>, List<Column>> parentTypeToCurrentType() {
        return familyOpt -> familyOpt.map(Family::getColumnsList).orElse(Collections.emptyList());
      }
    }
  }

  public interface ColumnsWithinFamiliesRead extends ColumnsRead<
      ColumnsWithinFamiliesRead, CellsWithinFamiliesRead, CellWithinFamiliesRead, List<Family>> {

    class ReadImpl extends MultiReadImpl<ColumnsWithinFamiliesRead, CellsWithinFamiliesRead, CellWithinFamiliesRead, Family>
        implements ColumnsWithinFamiliesRead {

      ReadImpl(final Internal<List<Family>> parentRead) {
        super(parentRead);
      }

      @Override
      ColumnsWithinFamiliesRead multiCol() {
        return this;
      }

      @Override
      public CellsWithinFamiliesRead cells() {
        return new CellsWithinFamiliesRead.ReadImpl(this);
      }

      @Override
      public CellWithinFamiliesRead latestCell() {
        return new CellWithinFamiliesRead.ReadImpl(this);
      }
    }
  }

  public interface ColumnsWithinRowsRead extends ColumnsRead<
      ColumnsWithinRowsRead, CellsWithinRowsRead, CellWithinRowsRead, List<Row>> {

    class ReadImpl extends MultiReadImpl<ColumnsWithinRowsRead, CellsWithinRowsRead, CellWithinRowsRead, Row>
        implements ColumnsWithinRowsRead {

      ReadImpl(final Internal<List<Row>> parent) {
        super(parent);
      }

      @Override
      ColumnsWithinRowsRead multiCol() {
        return this;
      }

      @Override
      public CellsWithinRowsRead cells() {
        return new CellsWithinRowsRead.ReadImpl(this);
      }

      @Override
      public CellWithinRowsRead latestCell() {
        return new CellWithinRowsRead.ReadImpl(this);
      }
    }
  }

  private abstract static class MultiReadImpl<MultiCol, MultiCell, OneCell, R>
      extends AbstractColumnsRead<MultiCol, MultiCell, OneCell, List<R>, List<R>> {

    private MultiReadImpl(final Internal<List<R>> parentRead) {
      super(parentRead);
    }

    @Override
    protected Function<List<R>, List<R>> parentTypeToCurrentType() {
      return Function.identity();
    }
  }

  private abstract static class AbstractColumnsRead<MultiCol, MultiCell, OneCell, R, P>
      extends AbstractBigtableRead<P, R> implements ColumnsRead<MultiCol, MultiCell, OneCell, R> {

    public AbstractColumnsRead(final Internal<P> parentRead) {
      super(parentRead);
    }

    abstract MultiCol multiCol();

    MultiCol columnQualifierRegex(final String columnQualifierRegex) {
      final ByteString columnRegexBytes = ByteString.copyFromUtf8(columnQualifierRegex);
      final RowFilter.Builder columnFilter = RowFilter.newBuilder().setColumnQualifierRegexFilter(columnRegexBytes);
      addRowFilter(columnFilter);
      return multiCol();
    }

    @Override
    public MultiCol startQualifierClosed(ByteString startQualifierClosed) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierClosed(startQualifierClosed);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return multiCol();
    }

    @Override
    public MultiCol startQualifierOpen(ByteString startQualifierOpen) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setStartQualifierOpen(startQualifierOpen);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return multiCol();
    }

    @Override
    public MultiCol endQualifierClosed(ByteString endQualifierClosed) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierClosed(endQualifierClosed);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return multiCol();
    }

    @Override
    public MultiCol endQualifierOpen(ByteString endQualifierOpen) {
      final ColumnRange.Builder columnRange = ColumnRange.newBuilder().setEndQualifierOpen(endQualifierOpen);
      addRowFilter(RowFilter.newBuilder().setColumnRangeFilter(columnRange));
      return multiCol();
    }
  }

}
