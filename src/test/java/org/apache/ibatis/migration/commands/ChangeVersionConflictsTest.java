/**
 *    Copyright 2010-2015 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.migration.commands;

import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.SqlRunner;
import org.apache.ibatis.migration.MigrationException;
import org.apache.ibatis.migration.options.SelectedOptions;
import org.apache.ibatis.migration.options.SelectedPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import static org.junit.Assert.*;

public class ChangeVersionConflictsTest {
  private SelectedOptions newSelectedOption;
  private SelectedPaths selectedPaths;

  public static final String BLOG_PROPERTIES = "databases/blog/blog-derby.properties";

  private static PrintStream out;
  private static StringOutputStream buffer;

  @Before
  public void setup() throws Exception {
    selectedPaths = new SelectedPaths();
    selectedPaths.setBasePath(new File("src/test/java/org/apache/ibatis/migration/commands"));

    newSelectedOption = new SelectedOptions();
    newSelectedOption.setCommand("New");
    newSelectedOption.setPaths(selectedPaths);

    out = System.out;
    buffer = new StringOutputStream();
    System.setOut(new PrintStream(buffer));
    ensureCleanTestDB();
    setupTestDB();
  }

  @After
  public void teardown() throws Exception {
    System.setOut(out);
    ensureCleanTestDB();
  }

  @Test
  public void sequenceNumberOverrideTest() {
    newSelectedOption.setEnvironment("development_useSeqNum");

    NewCommand newCommand = new NewCommand(newSelectedOption);
    newCommand.execute("should start with one");
    newCommand.execute("should start with two 1");

  File[] files = selectedPaths.getScriptPath().listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
          return name.indexOf("1")==0;
      }
    });

    assertTrue(files.length > 0);
    files[0].delete();
    assertTrue(files.length > 1);
    files[1].delete();
  }

  @Test
  public void testUpCommandDisplaysVersionConflict() {
    newSelectedOption.setEnvironment("development_useSeqNum");


    buffer.clear();
    StatusCommand statusCommand = new StatusCommand(newSelectedOption);
    statusCommand.execute();
    assertFalse(buffer.toString().contains("...pending..."));

    //Add Duplcate
    NewCommand newCommand = new NewCommand(newSelectedOption);
    newCommand.execute("should start with one first 1");
    newCommand.execute("should start with two first 2");
    newCommand.execute("should start with two duplicate 2");

    try {
      buffer.clear();
      //Migrate up to just before the conflict
      UpCommand upCommand = new UpCommand(newSelectedOption);
      upCommand.execute("2");
      upCommand = new UpCommand(newSelectedOption);
      upCommand.execute();
    } catch (MigrationException mx) {
      assertTrue(mx.getMessage().contains("Version conflict between changes [2] and [2]"));
      assertTrue(buffer.toString().contains("Version conflict between changes [2] and [2]"));

      buffer.clear();
      statusCommand = new StatusCommand(newSelectedOption);
      statusCommand.execute();
      assertTrue(buffer.toString().contains("should start with one first"));
      assertTrue(buffer.toString().contains("should start with two first"));
      assertTrue(buffer.toString().contains("should start with two duplicate"));
      assertTrue(buffer.toString().contains("...pending..."));

      try {
        buffer.clear();
        //Migrate down to test a conflict mid migration
        DownCommand downCommand = new DownCommand(newSelectedOption);
        downCommand.execute("2");
        UpCommand upCommand = new UpCommand(newSelectedOption);
        upCommand.execute();
      } catch (MigrationException mx2) {
        assertTrue(mx2.getMessage().contains("Version conflict between changes [2] and [2]"));
        assertTrue(buffer.toString().contains("Version conflict between changes [2] and [2]"));

        buffer.clear();
        statusCommand = new StatusCommand(newSelectedOption);
        statusCommand.execute();
        assertTrue(buffer.toString().contains("should start with one first"));
        assertTrue(buffer.toString().contains("should start with two first"));
        assertTrue(buffer.toString().contains("should start with two duplicate"));
        assertTrue(buffer.toString().contains("...pending..."));
      }
    } finally {
      //Cleanup
      File[] files = selectedPaths.getScriptPath().listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.indexOf("2")==0 || name.indexOf("1")==0;
        }
      });
  
      assertTrue(files.length > 0);
      files[0].delete();
      assertTrue(files.length > 1);
      files[1].delete();
      assertTrue(files.length > 2);
      files[2].delete();
    }
  }

  private static class StringOutputStream extends OutputStream {

    private StringBuilder builder = new StringBuilder();

    public void write(int b) throws IOException {
      builder.append((char) b);
//      out.write(b);
    }

    @Override
    public String toString() {
      return builder.toString();
    }

    public void clear() {
      builder.setLength(0);
    }
  }

  /**
   * @throws IOException
   * @throws SQLException
   */
  public static void ensureCleanTestDB() throws IOException, SQLException {
    DataSource ds = createUnpooledDataSource(BLOG_PROPERTIES);
    Connection conn = ds.getConnection();
    SqlRunner executor = new SqlRunner(conn);
    safeRun(executor, "DROP TABLE changelog");
    conn.commit();
    conn.close();
  }
  /**
   * @throws IOException
   * @throws SQLException
   */
  public static void setupTestDB() throws Exception {
    DataSource ds = createUnpooledDataSource(BLOG_PROPERTIES);
    Connection conn = ds.getConnection();
    SqlRunner executor = new SqlRunner(conn);
    safeRun(executor, "CREATE TABLE changelog (" +
        "ID NUMERIC(20,0) NOT NULL, " +
      "APPLIED_AT VARCHAR(25) NOT NULL, " +
      "DESCRIPTION VARCHAR(255) NOT NULL" +
      ")");
    conn.commit();
    try {
      final List<Map<String, Object>> change = executor.selectAll("select * from changelog ");
        assertTrue(change.size() == 0);
    } catch (Exception ex) {
      throw ex;
    }
    conn.close();
  }

  public static UnpooledDataSource createUnpooledDataSource(String resource) throws IOException {
    Properties props = Resources.getResourceAsProperties(resource);
    UnpooledDataSource ds = new UnpooledDataSource();
    ds.setDriver(props.getProperty("driver"));
    ds.setUrl(props.getProperty("url"));
    ds.setUsername(props.getProperty("username"));
    ds.setPassword(props.getProperty("password"));
    return ds;
  }
  
  private static void safeRun(SqlRunner executor, String sql) {
    try {
      executor.run(sql);
    } catch (Exception e) {
      //ignore
    }
  }
}