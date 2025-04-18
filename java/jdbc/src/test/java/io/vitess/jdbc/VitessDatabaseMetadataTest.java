/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.jdbc;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.protobuf.ByteString;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.proto.Query;
import io.vitess.util.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by ashudeep.sharma on 08/03/16.
 */
public class VitessDatabaseMetadataTest extends BaseTest {

  private ResultSet resultSet;

  @Test
  public void getPseudoColumnsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getPseudoColumns(null, null, null, null);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TABLE_SCHEM", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("TABLE_NAME", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("COLUMN_NAME", resultSetMetaData.getColumnName(4));
    Assert.assertEquals("DATA_TYPE", resultSetMetaData.getColumnName(5));
    Assert.assertEquals("COLUMN_SIZE", resultSetMetaData.getColumnName(6));
    Assert.assertEquals("DECIMAL_DIGITS", resultSetMetaData.getColumnName(7));
    Assert.assertEquals("NUM_PREC_RADIX", resultSetMetaData.getColumnName(8));
    Assert.assertEquals("COLUMN_USAGE", resultSetMetaData.getColumnName(9));
    Assert.assertEquals("REMARKS", resultSetMetaData.getColumnName(10));
    Assert.assertEquals("CHAR_OCTET_LENGTH", resultSetMetaData.getColumnName(11));
    Assert.assertEquals("IS_NULLABLE", resultSetMetaData.getColumnName(12));
  }

  @Test
  public void getClientInfoPropertiesTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getClientInfoProperties();

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("NAME", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("MAX_LEN", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("DEFAULT_VALUE", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("DESCRIPTION", resultSetMetaData.getColumnName(4));
  }

  @Test
  public void getSchemasTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getSchemas(null, null);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TABLE_CATALOG", resultSetMetaData.getColumnName(2));
  }

  @Test
  public void getAttributesTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getAttributes(null, null, null, null);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TYPE_CAT", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TYPE_SCHEM", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("TYPE_NAME", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("ATTR_NAME", resultSetMetaData.getColumnName(4));
    Assert.assertEquals("DATA_TYPE", resultSetMetaData.getColumnName(5));
    Assert.assertEquals("ATTR_TYPE_NAME", resultSetMetaData.getColumnName(6));
    Assert.assertEquals("ATTR_SIZE", resultSetMetaData.getColumnName(7));
    Assert.assertEquals("DECIMAL_DIGITS", resultSetMetaData.getColumnName(8));
    Assert.assertEquals("NUM_PREC_RADIX", resultSetMetaData.getColumnName(9));
    Assert.assertEquals("NULLABLE", resultSetMetaData.getColumnName(10));
    Assert.assertEquals("REMARKS", resultSetMetaData.getColumnName(11));
    Assert.assertEquals("ATTR_DEF", resultSetMetaData.getColumnName(12));
    Assert.assertEquals("SQL_DATA_TYPE", resultSetMetaData.getColumnName(13));
    Assert.assertEquals("SQL_DATETIME_SUB", resultSetMetaData.getColumnName(14));
    Assert.assertEquals("CHAR_OCTET_LENGTH", resultSetMetaData.getColumnName(15));
    Assert.assertEquals("ORDINAL_POSITION", resultSetMetaData.getColumnName(16));
    Assert.assertEquals("ISNULLABLE", resultSetMetaData.getColumnName(17));
    Assert.assertEquals("SCOPE_CATALOG", resultSetMetaData.getColumnName(18));
    Assert.assertEquals("SCOPE_SCHEMA", resultSetMetaData.getColumnName(19));
    Assert.assertEquals("SCOPE_TABLE", resultSetMetaData.getColumnName(20));
    Assert.assertEquals("SOURCE_DATA_TYPE", resultSetMetaData.getColumnName(21));
  }

  @Test
  public void getSuperTablesTest() throws SQLException {

    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getSuperTables(null, null, null);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TYPE_SCHEM", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("TABLE_NAME", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("SUPERTABLE_NAME", resultSetMetaData.getColumnName(4));
  }

  @Test
  public void getSuperTypesTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getSuperTypes(null, null, null);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TYPE_CAT", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TYPE_SCHEM", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("TYPE_NAME", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("SUPERTYPE_CAT", resultSetMetaData.getColumnName(4));
    Assert.assertEquals("SUPERTYPE_SCHEM", resultSetMetaData.getColumnName(5));
    Assert.assertEquals("SUPERTYPE_NAME", resultSetMetaData.getColumnName(6));
  }

  @Test
  public void getUDTsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getUDTs(null, null, null, null);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TYPE_CAT", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TYPE_SCHEM", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("TYPE_NAME", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("CLASS_NAME", resultSetMetaData.getColumnName(4));
    Assert.assertEquals("DATA_TYPE", resultSetMetaData.getColumnName(5));
    Assert.assertEquals("REMARKS", resultSetMetaData.getColumnName(6));
    Assert.assertEquals("BASE_TYPE", resultSetMetaData.getColumnName(7));
  }

  @Test
  public void getTypeInfoTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getTypeInfo();

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TYPE_NAME", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("DATA_TYPE", resultSetMetaData.getColumnName(2));
    Assert.assertEquals("PRECISION", resultSetMetaData.getColumnName(3));
    Assert.assertEquals("LITERAL_PREFIX", resultSetMetaData.getColumnName(4));
    Assert.assertEquals("LITERAL_SUFFIX", resultSetMetaData.getColumnName(5));
    Assert.assertEquals("CREATE_PARAMS", resultSetMetaData.getColumnName(6));
    Assert.assertEquals("NULLABLE", resultSetMetaData.getColumnName(7));
    Assert.assertEquals("CASE_SENSITIVE", resultSetMetaData.getColumnName(8));
    Assert.assertEquals("SEARCHABLE", resultSetMetaData.getColumnName(9));
    Assert.assertEquals("UNSIGNED_ATTRIBUTE", resultSetMetaData.getColumnName(10));
    Assert.assertEquals("FIXED_PREC_SCALE", resultSetMetaData.getColumnName(11));
    Assert.assertEquals("AUTO_INCREMENT", resultSetMetaData.getColumnName(12));
    Assert.assertEquals("LOCAL_TYPE_NAME", resultSetMetaData.getColumnName(13));
    Assert.assertEquals("MINIMUM_SCALE", resultSetMetaData.getColumnName(14));
    Assert.assertEquals("MAXIMUM_SCALE", resultSetMetaData.getColumnName(15));
    Assert.assertEquals("SQL_DATA_TYPE", resultSetMetaData.getColumnName(16));
    Assert.assertEquals("SQL_DATETIME_SUB", resultSetMetaData.getColumnName(17));
    Assert.assertEquals("NUM_PREC_RADIX", resultSetMetaData.getColumnName(18));

    //Check for ResultSet Data as well
  }

  @Test
  public void getTableTypesTest() throws SQLException {

    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getTableTypes();

    ArrayList<String> data = new ArrayList<String>();
    while (resultSet.next()) {
      data.add(resultSet.getString("table_type"));
    }

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("table_type", resultSetMetaData.getColumnName(1));
    //Checking Data
    Assert.assertEquals("LOCAL TEMPORARY", data.get(0));
    Assert.assertEquals("SYSTEM TABLES", data.get(1));
    Assert.assertEquals("SYSTEM VIEW", data.get(2));
    Assert.assertEquals("TABLE", data.get(3));
    Assert.assertEquals("VIEW", data.get(4));
  }

  @Test
  public void getSchemasTest2() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    this.resultSet = vitessDatabaseMetaData.getSchemas();

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Assert.assertEquals("TABLE_SCHEM", resultSetMetaData.getColumnName(1));
    Assert.assertEquals("TABLE_CATALOG", resultSetMetaData.getColumnName(2));
  }

  @Test
  public void allProceduresAreCallableTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.allProceduresAreCallable());
  }

  @Test
  public void allTablesAreSelectableTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.allTablesAreSelectable());
  }

  @Test
  public void nullsAreSortedHighTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.nullsAreSortedHigh());
    Assert.assertEquals(false, vitessMariaDBDatabaseMetadata.nullsAreSortedHigh());
  }

  @Test
  public void nullsAreSortedLowTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.nullsAreSortedLow());
    Assert.assertEquals(true, vitessMariaDBDatabaseMetadata.nullsAreSortedLow());
  }

  @Test
  public void nullsAreSortedAtStartTest() throws SQLException {
    VitessDatabaseMetaData vitessMySQLDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false, vitessMySQLDatabaseMetaData.nullsAreSortedAtStart());
    Assert.assertEquals(false, vitessMariaDBDatabaseMetadata.nullsAreSortedAtStart());
  }

  @Test
  public void nullsAreSortedAtEndTest() throws SQLException {
    VitessDatabaseMetaData vitessMySQLDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false, vitessMySQLDatabaseMetaData.nullsAreSortedAtEnd());
    Assert.assertEquals(true, vitessMariaDBDatabaseMetadata.nullsAreSortedAtEnd());
  }

  @Test
  public void getDatabaseProductNameTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("MySQL", vitessDatabaseMetaData.getDatabaseProductName());
  }

  @Test
  public void getDriverVersionTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    StringBuilder driverVersionBuilder = new StringBuilder();
    driverVersionBuilder.append(Constants.DRIVER_MAJOR_VERSION);
    driverVersionBuilder.append(".");
    driverVersionBuilder.append(Constants.DRIVER_MINOR_VERSION);
    Assert.assertEquals(driverVersionBuilder.toString(), vitessDatabaseMetaData.getDriverVersion());
  }

  @Test
  public void getDriverMajorVersionTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(Constants.DRIVER_MAJOR_VERSION,
        vitessDatabaseMetaData.getDriverMajorVersion());
  }

  @Test
  public void getDriverMinorVersionTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(Constants.DRIVER_MINOR_VERSION,
        vitessDatabaseMetaData.getDriverMinorVersion());
  }

  @Test
  public void getSearchStringEscapeTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("\\", vitessDatabaseMetaData.getSearchStringEscape());
  }

  @Test
  public void getExtraNameCharactersTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("#@", vitessDatabaseMetaData.getExtraNameCharacters());
  }

  @Test
  public void supportsAlterTableWithAddColumnTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsAlterTableWithAddColumn());
  }

  @Test
  public void supportsAlterTableWithDropColumnTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsAlterTableWithDropColumn());
  }

  @Test
  public void supportsColumnAliasingTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.supportsColumnAliasing());
  }

  @Test
  public void nullPlusNonNullIsNullTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.nullPlusNonNullIsNull());
  }

  @Test
  public void supportsExpressionsInOrderByTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsExpressionsInOrderBy());
  }

  @Test
  public void supportsOrderByUnrelatedTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsOrderByUnrelated());
  }

  @Test
  public void supportsGroupByTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsGroupBy());
  }

  @Test
  public void supportsGroupByUnrelatedTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsGroupByUnrelated());
  }

  @Test
  public void supportsGroupByBeyondSelectTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsGroupByBeyondSelect());
  }

  @Test
  public void supportsLikeEscapeClauseTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.supportsLikeEscapeClause());
  }

  @Test
  public void supportsMultipleResultSetsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsMultipleResultSets());
  }

  @Test
  public void supportsMultipleTransactionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.supportsMultipleTransactions());
  }

  @Test
  public void supportsNonNullableColumnsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.supportsNonNullableColumns());
  }

  @Test
  public void supportsMinimumSQLGrammarTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.supportsMinimumSQLGrammar());
  }

  @Test
  public void supportsCoreSQLGrammarTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsCoreSQLGrammar());
  }

  @Test
  public void supportsExtendedSQLGrammarTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsExtendedSQLGrammar());
  }

  @Test
  public void supportsOuterJoinsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsOuterJoins());
  }

  @Test
  public void supportsFullOuterJoinsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsFullOuterJoins());
  }

  @Test
  public void supportsLimitedOuterJoinsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsLimitedOuterJoins());
  }

  @Test
  public void getSchemaTermTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("", vitessDatabaseMetaData.getSchemaTerm());
  }

  @Test
  public void getProcedureTermTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("procedure", vitessDatabaseMetaData.getProcedureTerm());
  }

  @Test
  public void getCatalogTermTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("database", vitessDatabaseMetaData.getCatalogTerm());
  }

  @Test
  public void isCatalogAtStartTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true, vitessDatabaseMetaData.isCatalogAtStart());
  }

  @Test
  public void getCatalogSeparatorTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(".", vitessDatabaseMetaData.getCatalogSeparator());
  }

  @Test
  public void supportsSchemasInDataManipulationTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSchemasInDataManipulation());
  }

  @Test
  public void supportsSchemasInProcedureCallsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSchemasInProcedureCalls());
  }

  @Test
  public void supportsSchemasInTableDefinitionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSchemasInTableDefinitions());
  }

  @Test
  public void supportsSchemasInIndexDefinitionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSchemasInIndexDefinitions());
  }

  @Test
  public void supportsSelectForUpdateTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSelectForUpdate());
  }

  @Test
  public void supportsStoredProceduresTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsStoredProcedures());
  }

  @Test
  public void supportsSubqueriesInComparisonsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSubqueriesInComparisons());
  }

  @Test
  public void supportsSubqueriesInExistsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSubqueriesInExists());
  }

  @Test
  public void supportsSubqueriesInInsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSubqueriesInIns());
  }

  @Test
  public void supportsSubqueriesInQuantifiedsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsSubqueriesInQuantifieds());
  }

  @Test
  public void supportsCorrelatedSubqueriesTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsCorrelatedSubqueries());
  }

  @Test
  public void supportsUnionTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsUnion());
  }

  @Test
  public void supportsUnionAllTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsUnionAll());
  }

  @Test
  public void supportsOpenCursorsAcrossRollbackTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsOpenCursorsAcrossRollback());
  }

  @Test
  public void supportsOpenStatementsAcrossCommitTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsOpenStatementsAcrossCommit());
  }

  @Test
  public void supportsOpenStatementsAcrossRollbackTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsOpenStatementsAcrossRollback());
  }

  @Test
  public void supportsOpenCursorsAcrossCommitTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.supportsOpenCursorsAcrossCommit());
  }

  @Test
  public void getMaxBinaryLiteralLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(16777208, vitessDatabaseMetaData.getMaxBinaryLiteralLength());
  }

  @Test
  public void getMaxCharLiteralLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(16777208, vitessDatabaseMetaData.getMaxCharLiteralLength());
  }

  @Test
  public void getMaxColumnNameLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(64, vitessDatabaseMetaData.getMaxColumnNameLength());
  }

  @Test
  public void getMaxColumnsInGroupByTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(64, vitessDatabaseMetaData.getMaxColumnsInGroupBy());
  }

  @Test
  public void getMaxColumnsInIndexTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(16, vitessDatabaseMetaData.getMaxColumnsInIndex());
  }

  @Test
  public void getMaxColumnsInOrderByTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(64, vitessDatabaseMetaData.getMaxColumnsInOrderBy());
  }

  @Test
  public void getMaxColumnsInSelectTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(256, vitessDatabaseMetaData.getMaxColumnsInSelect());
  }

  @Test
  public void getMaxIndexLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(256, vitessDatabaseMetaData.getMaxIndexLength());
  }

  @Test
  public void doesMaxRowSizeIncludeBlobsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.doesMaxRowSizeIncludeBlobs());
  }

  @Test
  public void getMaxTableNameLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(64, vitessDatabaseMetaData.getMaxTableNameLength());
  }

  @Test
  public void getMaxTablesInSelectTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(256, vitessDatabaseMetaData.getMaxTablesInSelect());
  }

  @Test
  public void getMaxUserNameLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(16, vitessDatabaseMetaData.getMaxUserNameLength());
  }

  @Test
  public void supportsDataDefinitionAndDataManipulationTransactionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions());
  }

  @Test
  public void dataDefinitionCausesTransactionCommitTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.dataDefinitionCausesTransactionCommit());
  }

  @Test
  public void dataDefinitionIgnoredInTransactionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.dataDefinitionIgnoredInTransactions());
  }

  @Test
  public void getIdentifierQuoteStringTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals("`", vitessDatabaseMetaData.getIdentifierQuoteString());
  }

  @Test
  public void getProceduresTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(null, vitessDatabaseMetaData.getProcedures(null, null, null));
  }

  @Test
  public void supportsResultSetTypeTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(true,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.CONCUR_UPDATABLE));
    Assert
        .assertEquals(false, vitessDatabaseMetaData.supportsResultSetType(ResultSet.FETCH_FORWARD));
    Assert
        .assertEquals(false, vitessDatabaseMetaData.supportsResultSetType(ResultSet.FETCH_REVERSE));
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsResultSetType(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    Assert
        .assertEquals(false, vitessDatabaseMetaData.supportsResultSetType(ResultSet.FETCH_UNKNOWN));
  }

  @Test
  public void supportsResultSetConcurrencyTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    Assert.assertEquals(true, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.CLOSE_CURSORS_AT_COMMIT,
            ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.FETCH_REVERSE, ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.HOLD_CURSORS_OVER_COMMIT,
            ResultSet.CONCUR_READ_ONLY));
    Assert.assertEquals(false, vitessDatabaseMetaData
        .supportsResultSetConcurrency(ResultSet.FETCH_UNKNOWN, ResultSet.CONCUR_READ_ONLY));
  }

  @Test
  public void getJDBCMajorVersionTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(1, vitessDatabaseMetaData.getJDBCMajorVersion());
  }

  @Test
  public void getJDBCMinorVersionTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(0, vitessDatabaseMetaData.getJDBCMinorVersion());
  }

  @Test
  public void getNumericFunctionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(
        "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,"
            + "MOD,PI,POW,POWER,"
            + "RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE",
        vitessDatabaseMetaData.getNumericFunctions());
  }

  @Test
  public void getStringFunctionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(
        "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONV,ELT,"
            + "EXPORT_SET,FIELD,"
            + "FIND_IN_SET,HEX,INSERT,INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,"
            + "LTRIM,MAKE_SET,"
            + "MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,"
            + "RTRIM,SOUNDEX,SPACE,"
            + "STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING_INDEX,TRIM,UCASE,UPPER",
        vitessDatabaseMetaData.getStringFunctions());
  }

  @Test
  public void getSystemFunctionsTest() throws SQLException {
    VitessDatabaseMetaData vitessMySQLDatabaseMetadata = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatbaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(
        "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION,PASSWORD,ENCRYPT",
        vitessMySQLDatabaseMetadata.getSystemFunctions());
    Assert.assertEquals("DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION",
        vitessMariaDBDatbaseMetadata.getSystemFunctions());
  }

  @Test
  public void getTimeDateFunctionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(
        "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,"
            + "MINUTE,SECOND,"
            + "PERIOD_ADD,PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,CURDATE,"
            + "CURRENT_DATE,CURTIME,"
            + "CURRENT_TIME,NOW,SYSDATE,CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,"
            + "SEC_TO_TIME,TIME_TO_SEC",
        vitessDatabaseMetaData.getTimeDateFunctions());
  }

  @Test
  public void autoCommitFailureClosesAllResultSetsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.autoCommitFailureClosesAllResultSets());
  }

  @Test
  public void getUrlTest() throws SQLException {
    String connectionUrl = "jdbc:vitess://<vtGateHostname>:<vtGatePort>/<keyspace>/<dbName>";
    VitessJDBCUrl mockUrl = Mockito.mock(VitessJDBCUrl.class);
    Mockito.when(mockUrl.getUrl()).thenReturn(connectionUrl);

    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    Mockito.when(mockConn.getUrl()).thenReturn(mockUrl);

    Assert.assertEquals(connectionUrl, mockConn.getUrl().getUrl());
  }

  @Test
  public void isReadOnlyTest() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    Mockito.when(mockConn.isReadOnly()).thenReturn(false);
    Assert.assertEquals(false, mockConn.isReadOnly());

  }

  @Test
  public void getDriverNameTest() throws SQLException {
    VitessDatabaseMetaData vitessMySQLDatabaseMetadata = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals("Vitess MySQL JDBC Driver", vitessMySQLDatabaseMetadata.getDriverName());
    Assert
        .assertEquals("Vitess MariaDB JDBC Driver", vitessMariaDBDatabaseMetadata.getDriverName());
  }

  @Test
  public void usesLocalFilesTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.usesLocalFiles());
    Assert.assertEquals(false, vitessDatabaseMetaData.usesLocalFilePerTable());
    Assert.assertEquals(false, vitessDatabaseMetaData.storesUpperCaseIdentifiers());
  }

  @Test
  public void storeIdentifiersTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

    Assert.assertEquals(false, vitessDatabaseMetaData.storesUpperCaseIdentifiers());
  }

  @Test
  public void supportsTransactionsTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    Assert.assertEquals(true, vitessDatabaseMetaData.supportsTransactions());
  }

  @Test
  public void supportsTransactionIsolationLevelTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    Assert.assertEquals(false,
        vitessDatabaseMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
    Assert.assertEquals(true, vitessDatabaseMetaData
        .supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    Assert.assertEquals(true, vitessDatabaseMetaData
        .supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
    Assert.assertEquals(true, vitessDatabaseMetaData
        .supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
    Assert.assertEquals(true, vitessDatabaseMetaData
        .supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
  }

  @Test
  public void getMaxProcedureNameLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    Assert.assertEquals(256, vitessDatabaseMetaData.getMaxProcedureNameLength());
  }

  @Test
  public void getMaxCatalogNameLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(32, vitessMySqlDatabaseMetaData.getMaxCatalogNameLength());
    Assert.assertEquals(0, vitessMariaDBDatabaseMetadata.getMaxCatalogNameLength());
  }

  @Test
  public void getMaxRowSizeTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(2147483639, vitessMySqlDatabaseMetaData.getMaxRowSize());
    Assert.assertEquals(0, vitessMariaDBDatabaseMetadata.getMaxRowSize());
  }

  @Test
  public void getMaxStatementLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(65531, vitessMySqlDatabaseMetaData.getMaxStatementLength());
    Assert.assertEquals(0, vitessMariaDBDatabaseMetadata.getMaxStatementLength());
  }

  @Test
  public void getMaxStatementsTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(0, vitessMySqlDatabaseMetaData.getMaxStatements());
    Assert.assertEquals(0, vitessMariaDBDatabaseMetadata.getMaxStatements());
  }

  @Test
  public void supportsDataManipulationTransactionsOnlyTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false,
        vitessMySqlDatabaseMetaData.supportsDataManipulationTransactionsOnly());
    Assert.assertEquals(false,
        vitessMariaDBDatabaseMetadata.supportsDataManipulationTransactionsOnly());
  }

  @Test
  public void getMaxSchemaNameLengthTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(0, vitessMySqlDatabaseMetaData.getMaxSchemaNameLength());
    Assert.assertEquals(32, vitessMariaDBDatabaseMetadata.getMaxSchemaNameLength());
  }

  @Test
  public void supportsSavepointsTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false, vitessMySqlDatabaseMetaData.supportsSavepoints());
    Assert.assertEquals(false, vitessMariaDBDatabaseMetadata.supportsSavepoints());
  }

  @Test
  public void supportsMultipleOpenResultsTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false, vitessMySqlDatabaseMetaData.supportsMultipleOpenResults());
    Assert.assertEquals(false, vitessMariaDBDatabaseMetadata.supportsMultipleOpenResults());
  }

  @Test
  public void locatorsUpdateCopyTest() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(true, vitessMySqlDatabaseMetaData.locatorsUpdateCopy());
    Assert.assertEquals(false, vitessMariaDBDatabaseMetadata.locatorsUpdateCopy());
  }

  @Test
  public void supportsStatementPooling() throws SQLException {
    VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
    VitessDatabaseMetaData vitessMariaDBDatabaseMetadata = new VitessMariaDBDatabaseMetadata(null);

    Assert.assertEquals(false, vitessMySqlDatabaseMetaData.supportsStatementPooling());
    Assert.assertEquals(false, vitessMariaDBDatabaseMetadata.supportsStatementPooling());
  }

  private Cursor getTablesCursor() throws Exception {
    return new SimpleCursor(Query.QueryResult.newBuilder()
        .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("TABLE_TYPE").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("REMARKS").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("TYPE_CAT").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("TYPE_SCHEM").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("TYPE_NAME").setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("SELF_REFERENCING_COL_NAME")
            .setType(Query.Type.VARCHAR))
        .addFields(Query.Field.newBuilder().setName("REF_GENERATION").setType(Query.Type.VARCHAR))
        .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
            .addLengths("SampleTable1".length()).addLengths("TABLE".length())
            .addLengths("".length()).addLengths("".length()).addLengths("".length())
            .addLengths("".length()).addLengths("".length()).addLengths("".length())
            .setValues(ByteString.copyFromUtf8("TestDB1sampleTable1TABLE"))).addRows(
            Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleView1".length()).addLengths("VIEW".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB1SampleView1VIEW"))).addRows(
            Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleSystemView".length()).addLengths("SYSTEM VIEW".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB2SampleSystemViewSYSTEM VIEW"))).addRows(
            Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleSystemTable".length()).addLengths("SYSTEM TABLE".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB2SampleSystemTableSYSTEM TABLE")))
        .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
            .addLengths("SampleLocalTemporary".length()).addLengths("LOCAL TEMPORARY".length())
            .addLengths("".length()).addLengths("".length()).addLengths("".length())
            .addLengths("".length()).addLengths("".length()).addLengths("".length())
            .setValues(ByteString.copyFromUtf8("TestDB2SampleLocalTemporaryLOCAL TEMPORARY")))
        .build());
  }

  private void assertResultSetEquals(ResultSet actualResultSet, ResultSet expectedResultSet)
      throws SQLException {
    ResultSetMetaData actualResultSetMetadata = actualResultSet.getMetaData();
    ResultSetMetaData expectedResultSetMetadata = expectedResultSet.getMetaData();
    //Column Count Comparison
    Assert.assertEquals(expectedResultSetMetadata.getColumnCount(),
        actualResultSetMetadata.getColumnCount());
    //Column Type Comparison
    for (int i = 0; i < expectedResultSetMetadata.getColumnCount(); i++) {
      Assert.assertEquals(expectedResultSetMetadata.getColumnType(i + 1),
          actualResultSetMetadata.getColumnType(i + 1));
    }

    //Actual Values Comparison
    while (expectedResultSet.next() && actualResultSet.next()) {
      for (int i = 0; i < expectedResultSetMetadata.getColumnCount(); i++) {
        switch (expectedResultSetMetadata.getColumnType(i + 1)) {
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
            Assert.assertEquals(expectedResultSet.getInt(i + 1), actualResultSet.getInt(i + 1));
            break;
          case Types.BIGINT:
            Assert.assertEquals(expectedResultSet.getLong(i + 1), actualResultSet.getLong(i + 1));
            break;
          case Types.FLOAT:
            Assert.assertEquals(expectedResultSet.getFloat(i + 1), actualResultSet.getFloat(i + 1),
                0.1);
            break;
          case Types.DOUBLE:
            Assert
                .assertEquals(expectedResultSet.getDouble(i + 1), actualResultSet.getDouble(i + 1),
                    0.1);
            break;
          case Types.TIME:
            Assert.assertEquals(expectedResultSet.getTime(i + 1), actualResultSet.getTime(i + 1));
            break;
          case Types.TIMESTAMP:
            Assert.assertEquals(expectedResultSet.getTimestamp(i + 1),
                actualResultSet.getTimestamp(i + 1));
            break;
          case Types.DATE:
            Assert.assertEquals(expectedResultSet.getDate(i + 1), actualResultSet.getDate(i + 1));
            break;
          case Types.BLOB:
            Assert.assertEquals(expectedResultSet.getBlob(i + 1), actualResultSet.getBlob(i + 1));
            break;
          case Types.BINARY:
          case Types.LONGVARBINARY:
            Assert.assertEquals(expectedResultSet.getBytes(i + 1), actualResultSet.getBytes(i + 1));
            break;
          default:
            Assert
                .assertEquals(expectedResultSet.getString(i + 1), actualResultSet.getString(i + 1));
            break;
        }
      }
    }
  }

  @Test
  public void getUserNameTest() {
    try {
      VitessConnection vitessConnection = new VitessConnection(
          "jdbc:vitess://username@ip1:port1/keyspace", null);
      VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(
          vitessConnection);
      Assert.assertEquals("username", vitessDatabaseMetaData.getUserName());

      vitessConnection = new VitessConnection("jdbc:vitess://ip1:port1/keyspace", null);
      vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(vitessConnection);
      Assert.assertEquals(null, vitessDatabaseMetaData.getUserName());

      Properties properties = new Properties();
      properties.put(Constants.Property.USERNAME, "username");
      vitessConnection = new VitessConnection("jdbc:vitess://ip1:port1/keyspace", properties);
      vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(vitessConnection);
      Assert.assertEquals("username", vitessDatabaseMetaData.getUserName());

    } catch (SQLException e) {
      Assert.fail("Exception occurred: " + e.getMessage());
    }
  }

  /**
   * Tests parsing all the various outputs of SHOW CREATE TABLE for the foreign key constraints.
   */
  @Test
  public void extractForeignKeyForTableTest() throws SQLException, IOException {
    VitessConnection vitessConnection = new VitessConnection(
        "jdbc:vitess://username@ip1:port1/keyspace", null);
    VitessMySQLDatabaseMetadata vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(
        vitessConnection);

    try (InputStream resourceAsStream = this.getClass()
        .getResourceAsStream("/extractForeignKeyForTableTestCases.sql")) {
      Scanner scanner = new Scanner(resourceAsStream);
      List<ArrayList<String>> rows = new ArrayList<>();
      String testName = null;
      String testExpected = null;
      String testInput = "";
      String startTag = "-- name: ";
      String expectedTag = "-- expected: ";
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        if (line.startsWith(startTag)) {
          if (testName != null) {
            rows.clear();
            vitessDatabaseMetaData.extractForeignKeyForTable(rows, testInput, "test", "testA");
            assertForeignKeysOutput(testName, testExpected, rows);
            testInput = "";
          }
          testName = line.substring(startTag.length());
        } else if (line.startsWith(expectedTag)) {
          testExpected = line.substring(expectedTag.length());
        } else if (line.startsWith("--") || line.trim().isEmpty()) {
          // Just general comment or whitespace, we can ignore
        } else {
          testInput += line + "\n";
        }
      }

      rows.clear();
      vitessDatabaseMetaData.extractForeignKeyForTable(rows, testExpected, "test", "testA");
      assertForeignKeysOutput(testName, testExpected, rows);
    }
  }

  private void assertForeignKeysOutput(String testName, String expected,
      List<ArrayList<String>> output) {
    // Uncomment below for debugging
    //System.out.println("Name: " + testName);
    //System.out.println("Expected: " + expected);
    //System.out.println("Output: " + String.valueOf(output));
    Assert.assertEquals(testName, expected, String.valueOf(output));
  }
}
