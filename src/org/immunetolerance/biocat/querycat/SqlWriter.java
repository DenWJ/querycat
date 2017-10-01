package org.immunetolerance.biocat.querycat;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * Reads specimens from db of record
 *
 * @author denw
 */
class SqlWriter
{
    private volatile static SqlWriter reader = null;
    private Connection connection;

    static SqlWriter getInstance()
    {
        if (reader == null)
        {
            synchronized (SqlWriter.class)
            {
                if (reader == null)
                {
                    reader = new SqlWriter();
                }
            }
        }
        return reader;
    }

    private SqlWriter()
    {

    }

    boolean getConnection(String url)
    {
        try
        {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            connection = DriverManager.getConnection(url);

            return true;
        }
        catch (SQLException e)
        {
            System.out.println("Database connection: Failed for " + url + " e: " + e.getMessage() + " code: " + e.getErrorCode() + " state: " + e.getSQLState());
            return false;
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("ClassNotFoundException.  Make sure you have MS jdbc4 jar.");
            e.printStackTrace();
            return true;
        }
    }

    boolean closeConnection()
    {
        try
        {
            connection.close();
            return true;
        }
        catch (SQLException | NullPointerException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    void createTable(String tableName, Map<String, DbCol> columns, String schema) throws SQLException
    {

        Statement statement;

        String sql = "DROP TABLE " + schema + "." + tableName + "; CREATE TABLE " + schema + "." + tableName + "( ";

        for (String cname : columns.keySet())
        {
            sql += "[" + cname + "] " + columns.get(cname).getSqlType() + " NULL, ";
        }
        sql = sql.substring(0, sql.lastIndexOf(","));

        sql += ")";

        System.out.println("creating table: ");
        System.out.println(sql);

        try
        {
            statement = connection.createStatement();

            statement.execute(sql);

            System.out.println(tableName + " is created");

        }
        catch (SQLException e)
        {

            System.out.println(e.getMessage());

        }

    }

    // need to change iterators to write 10 or 100 rows (or pass in an arg to set the number of rows at a time)
    void update(Map<String, DbCol> cols, Set<Set<DbField>> results, String tablename, String schema)
    {
        String sql = null;
        try
        {
            PreparedStatement preparedStatement;
            int rowidx = 0;

            for (Set<DbField> row : results)
            {
                // can this section be moved outside the row iterator?
                sql = "INSERT INTO " + schema + "." + tablename + "(";
                for (DbField df : row)
                {
                    sql += "[" + df.columnName + "], ";
                }
                sql = sql.substring(0, sql.lastIndexOf(","));
                sql += ") VALUES (";

                for (int x = 0; x < row.size(); x++)
                {
                    sql += "?, ";
                }
                sql = sql.substring(0, sql.lastIndexOf(","));
                sql += ")";
                preparedStatement = connection.prepareStatement(sql);
                // end can this section be moved outside the row iterator?

                int idx = 1;
                for (DbField df : row)
                {
                    String type = cols.get(df.columnName).type;
                    if (type != null)
                    {
                        switch (cols.get(df.columnName).type)
                        {
                            case "string":
                                if (df.value != null)
                                {
                                    preparedStatement.setString(idx, df.value.toString());
                                }
                                else
                                {
                                    preparedStatement.setNull(idx, Types.VARCHAR);
                                }
                                break;
                            case "date":
                                if (df.value != null)
                                {
                                    java.sql.Timestamp sdate = Timestamp.from((Instant) df.value);
                                    preparedStatement.setTimestamp(idx, sdate);
                                }
                                else
                                {
                                    preparedStatement.setNull(idx, Types.TIMESTAMP);
                                }
                                break;
                            case "int":
                                if (df.value != null && NumberUtils.isCreatable(df.value.toString()))
                                {
                                    preparedStatement.setInt(idx, Integer.parseInt(df.value.toString()));
                                }
                                else
                                {
                                    preparedStatement.setNull(idx, Types.INTEGER);
                                }
                                break;
                            case "float":
                                if (df.value != null && NumberUtils.isCreatable(df.value.toString()))
                                {
                                    preparedStatement.setFloat(idx, Float.parseFloat(df.value.toString()));
                                }
                                else
                                {
                                    preparedStatement.setNull(idx, Types.FLOAT);
                                }
                                break;
                            case "boolean":
                                if (df.value != null && BooleanUtils.toBooleanObject(df.value.toString()) != null)
                                {
                                    preparedStatement.setBoolean(idx, BooleanUtils.toBooleanObject(df.value.toString()));
                                }
                                else
                                {
                                    preparedStatement.setNull(idx, Types.FLOAT);
                                }
                                break;
                            default:
                                System.err.println("Column " + df.columnName + " type was " + type + ".  This column won't be loaded.");
                        }
                    }
                    else
                    {
                        System.err.println(df.columnName + " has a null column type.  May not have found a match in the column set. This column won't be loaded.");
                    }
                    idx++;
                }

                // execute insert SQL stetement

                // System.out.println("Record is inserted into table "+tablename);


                int result = preparedStatement.executeUpdate();
                rowidx++;
                System.out.println(rowidx + "/" + results.size() + " inserted. result = " + result);
            }
        }
        catch (SQLException e)
        {
            System.out.println("sql update statement was " + sql);
            System.out.println(e.getMessage());
        }
    }

    void createSchema(String study) throws SQLException {
        Statement statement;

        String sql = String.format(
                        "IF NOT EXISTS (" +
                        "SELECT schema_name " +
                        "FROM information_schema.schemata " +
                        "WHERE schema_name = '%s') " +
                        "BEGIN " +
                        "EXEC sp_executesql N'CREATE SCHEMA %s' " +
                        "END", study, study);

        System.out.println("Creating schema \"" + study + "\": " + sql);

        try {
            statement = connection.createStatement();
            statement.execute(sql);
            System.out.println("Schema created");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
