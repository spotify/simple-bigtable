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
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.read.ReadCell.CellWithinCellsRead;
import com.spotify.bigtable.read.ReadCell.CellWithinFamiliesRead;
import com.spotify.bigtable.read.ReadCell.CellWithinRowsRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinColumnRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinFamiliesRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinRowsRead;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ReadColumn {

  interface ColumnRead<MultiCell, OneCell, R> extends BigtableRead<R> {

    MultiCell cells();

    OneCell latestCell();

  }

  public interface ColumnWithinFamilyRead
      extends ColumnRead<CellsWithinColumnRead, CellWithinCellsRead, Optional<Column>> {

    class ReadImpl
        extends AbstractColumnRead<ColumnWithinFamilyRead, CellsWithinColumnRead, CellWithinCellsRead, Optional<Column>, Optional<Family>>
        implements ColumnWithinFamilyRead {

      ReadImpl(final Internal<Optional<Family>> parentRead) {
        super(parentRead);
      }

      @Override
      protected ColumnWithinFamilyRead oneCol() {
        return this;
      }

      @Override
      public CellWithinCellsRead latestCell() {
        return cells().latest();
      }

      @Override
      public CellsWithinColumnRead cells() {
        return new CellsWithinColumnRead.ReadImpl(this);
      }

      @Override
      protected Function<Optional<Family>, Optional<Column>> parentTypeToCurrentType() {
        return familyOpt -> familyOpt.flatMap(family -> headOption(family.getColumnsList()));
      }
    }
  }

  public interface ColumnWithinFamiliesRead
      extends ColumnRead<CellsWithinFamiliesRead, CellWithinFamiliesRead, List<Family>> {

    class ReadImpl
        extends MultiReadImpl<ColumnWithinFamiliesRead, CellsWithinFamiliesRead, CellWithinFamiliesRead, Family>
        implements ColumnWithinFamiliesRead {

      ReadImpl(final Internal<List<Family>> parent) {
        super(parent);
      }

      @Override
      protected ColumnWithinFamiliesRead oneCol() {
        return this;
      }

      @Override
      public CellWithinFamiliesRead latestCell() {
        return cells().latest();
      }

      @Override
      public CellsWithinFamiliesRead cells() {
        return new CellsWithinFamiliesRead.ReadImpl(this);
      }
    }
  }


  public interface ColumnWithinRowsRead
      extends ColumnRead<CellsWithinRowsRead, CellWithinRowsRead, List<Row>> {

    class ReadImpl
        extends MultiReadImpl<ColumnWithinRowsRead, CellsWithinRowsRead, CellWithinRowsRead, Row>
        implements ColumnWithinRowsRead {

      ReadImpl(final Internal<List<Row>> parentRead) {
        super(parentRead);
      }

      @Override
      protected ColumnWithinRowsRead oneCol() {
        return this;
      }

      @Override
      public CellWithinRowsRead latestCell() {
        return cells().latest();
      }

      @Override
      public CellsWithinRowsRead cells() {
        return new CellsWithinRowsRead.ReadImpl(this);
      }
    }
  }

  private abstract static class MultiReadImpl<OneCol, MultiCell, OneCell, R>
      extends AbstractColumnRead<OneCol, MultiCell, OneCell, List<R>, List<R>> {

    MultiReadImpl(final Internal<List<R>> parentRead) {
      super(parentRead);
    }

    @Override
    protected Function<List<R>, List<R>> parentTypeToCurrentType() {
      return Function.identity();
    }
  }

  private abstract static class AbstractColumnRead<OneCol, MultiCell, OneCell, R, P>
      extends AbstractBigtableRead<P, R> implements ColumnRead<MultiCell, OneCell, R> {

    private AbstractColumnRead(final Internal<P> parentRead) {
      super(parentRead);
    }

    abstract protected OneCol oneCol();

    OneCol columnQualifier(final String columnQualifier) {
      final RowFilter.Builder qualifierFilter = RowFilter.newBuilder()
          .setColumnQualifierRegexFilter(ByteString.copyFromUtf8(toExactMatchRegex(columnQualifier)));
      addRowFilter(qualifierFilter);
      return oneCol();
    }
  }
}
