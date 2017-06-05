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
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;
import com.spotify.bigtable.read.ReadCell.CellWithinCellsRead;
import com.spotify.bigtable.read.ReadCell.CellWithinFamiliesRead;
import com.spotify.bigtable.read.ReadCell.CellWithinRowsRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinColumnRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinFamiliesRead;
import com.spotify.bigtable.read.ReadCells.CellsWithinRowsRead;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
      extends ColumnRead<CellsWithinFamiliesRead, CellWithinFamiliesRead, Map<String, Column>> {

    class ReadImpl
        extends AbstractColumnRead<ColumnWithinFamiliesRead, CellsWithinFamiliesRead, CellWithinFamiliesRead, Map<String, Column>, List<Family>>
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

      @Override
      protected Function<List<Family>, Map<String, Column>> parentTypeToCurrentType() {
        return families -> families.stream()
            .filter(family -> family.getColumnsCount() > 0)
            .collect(Collectors.toMap(Family::getName, fam -> fam.getColumns(0)));
      }
    }
  }


  public interface ColumnWithinRowsRead
      extends ColumnRead<CellsWithinRowsRead, CellWithinRowsRead, Map<String, Column>> {

    class ReadImpl
        extends AbstractColumnRead<ColumnWithinRowsRead, CellsWithinRowsRead, CellWithinRowsRead, Map<String, Column>, Map<String, Family>>
        implements ColumnWithinRowsRead {

      ReadImpl(final Internal<Map<String, Family>> parentRead) {
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

      @Override
      protected Function<Map<String, Family>, Map<String, Column>> parentTypeToCurrentType() {
        return input -> input.entrySet().stream()
            .filter(entry -> entry.getValue().getColumnsCount() > 0)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getColumns(0)));
      }
    }
  }

  public interface ColumnWithinFamiliesAndRowsRead
      extends ColumnRead<CellsWithinRowsRead, CellWithinRowsRead, Map<String, Map<String, Column>>> {

    class ReadImpl
        extends AbstractColumnRead<ColumnWithinRowsRead, CellsWithinRowsRead, CellWithinRowsRead, Map<String, Column>, Map<String, Family>>
        implements ColumnWithinRowsRead {

      ReadImpl(final Internal<Map<String, Family>> parentRead) {
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

      @Override
      protected Function<Map<String, Family>, Map<String, Column>> parentTypeToCurrentType() {
        return input -> input.entrySet().stream()
            .filter(entry -> entry.getValue().getColumnsCount() > 0)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getColumns(0)));
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
