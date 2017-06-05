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

import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.spotify.bigtable.read.ReadColumn.ColumnWithinFamilyRead;
import com.spotify.bigtable.read.ReadColumn.ColumnWithinRowsRead;
import com.spotify.bigtable.read.ReadColumns.ColumnsWithinFamilyRead;
import com.spotify.bigtable.read.ReadColumns.ColumnsWithinRowsRead;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReadFamily {

  interface FamilyRead<OneCol, MultiCol, R> extends BigtableRead<R> {

    OneCol columnQualifier(final String columnQualifier);

    MultiCol columnQualifierRegex(final String columnQualifierRegex);

    MultiCol columnsQualifiers(final Collection<String> columnQualifiers);

    MultiCol columns();
  }

  public interface FamilyWithinRowRead
      extends FamilyRead<ColumnWithinFamilyRead, ColumnsWithinFamilyRead, Optional<Family>> {

    class ReadImpl
        extends AbstractFamilyRead<FamilyWithinRowRead, ColumnWithinFamilyRead, ColumnsWithinFamilyRead, Optional<Family>, Optional<Row>>
        implements FamilyWithinRowRead {

      ReadImpl(final Internal<Optional<Row>> parentRead) {
        super(parentRead);
      }

      @Override
      protected FamilyWithinRowRead oneFam() {
        return this;
      }

      @Override
      public ColumnWithinFamilyRead columnQualifier(final String columnQualifier) {
        return new ColumnWithinFamilyRead.ReadImpl(this).columnQualifier(columnQualifier);
      }

      @Override
      public ColumnsWithinFamilyRead columnQualifierRegex(String columnQualifierRegex) {
        return new ColumnsWithinFamilyRead.ReadImpl(this)
            .columnQualifierRegex(columnQualifierRegex);
      }

      @Override
      public ColumnsWithinFamilyRead columns() {
        return new ColumnsWithinFamilyRead.ReadImpl(this);
      }

      @Override
      protected Function<Optional<Row>, Optional<Family>> parentTypeToCurrentType() {
        return rowOpt -> rowOpt.flatMap(row -> headOption(row.getFamiliesList()));
      }
    }
  }

  public interface FamilyWithinRowsRead
      extends FamilyRead<ColumnWithinRowsRead, ColumnsWithinRowsRead, Map<String, Family>> {

    class ReadImpl
        extends AbstractFamilyRead<FamilyWithinRowsRead, ColumnWithinRowsRead, ColumnsWithinRowsRead, Map<String, Family>, List<Row>>
        implements FamilyWithinRowsRead {

      ReadImpl(final Internal<List<Row>> parent) {
        super(parent);
      }

      @Override
      protected FamilyWithinRowsRead oneFam() {
        return this;
      }

      @Override
      public ColumnWithinRowsRead columnQualifier(final String columnQualifier) {
        return new ColumnWithinRowsRead.ReadImpl(this).columnQualifier(columnQualifier);
      }

      @Override
      public ColumnsWithinRowsRead columnQualifierRegex(final String columnQualifierRegex) {
        return new ColumnsWithinRowsRead.ReadImpl(this)
            .columnQualifierRegex(columnQualifierRegex);
      }

      @Override
      public ColumnsWithinRowsRead columns() {
        return new ColumnsWithinRowsRead.ReadImpl(this);
      }

      @Override
      protected Function<List<Row>, Map<String, Family>> parentTypeToCurrentType() {
        return input -> input.stream()
            .filter(row -> row.getFamiliesCount() > 0)
            .collect(Collectors.toMap(row -> row.getKey().toStringUtf8(),
                                      row -> row.getFamilies(0)));
      }
    }
  }

  private abstract static class AbstractFamilyRead<OneFam, OneCol, MultiCol, R, P>
      extends AbstractBigtableRead<P, R> implements FamilyRead<OneCol, MultiCol, R> {

    private AbstractFamilyRead(final Internal<P> parentRead) {
      super(parentRead);
    }

    abstract protected OneFam oneFam();

    OneFam columnFamily(final String columnFamily) {
      final RowFilter.Builder familyFilter = RowFilter.newBuilder()
          .setFamilyNameRegexFilter(toExactMatchRegex(columnFamily));
      addRowFilter(familyFilter);
      return oneFam();
    }

    @Override
    public MultiCol columnsQualifiers(Collection<String> columnQualifiers) {
      return columnQualifierRegex(toExactMatchAnyRegex(columnQualifiers));
    }
  }
}
