package com.alibaba.otter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import java.sql.*;
import com.microsoft.sqlserver.jdbc.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class MSSQLUtil {

    private static Connection connection = null;   //連接object
    private static Statement statement = null;

    private static String connectionString =
            "jdbc:sqlserver://XXX.XXX.XXX.XXX:1433;"
                    + "database=XXXXXX;"
                    + "user=XXXXXX;"
                    + "password=XXXXXX;"
                    + "encrypt=false;"
                    + "trustServerCertificate=false;"
                    //+ "hostNameInCertificate=*.database.windows.net;"
                    + "loginTimeout=30;";


    public static void Delete(CanalEntry.Entry entry, List<CanalEntry.Column> columns)
    {
        String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlColumn = new StringBuilder();
        sql.append("DELETE FROM  `"+ dbName + "`.`"+ tableName + "`  ");
        int index = 0;
        for (CanalEntry.Column column : columns) {
            if (index > 0) {
                sqlColumn.append(" AND ");
            }
            sqlColumn.append(column.getName() + " = \'\'" + column.getValue() + "\'\'");
            index++;
        }

        sql.append(" WHERE " + sqlColumn.toString());
        System.out.println(sql.toString());
        if(columns.size()>0) {
            Exec(GenInsertSQL(tableName, sql.toString()));
        }
    }

    public static void Update(CanalEntry.Entry entry, List<CanalEntry.Column> columns)
    {
        String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlColumn = new StringBuilder();
        StringBuilder sqlKey = new StringBuilder();
        int index = 0;
        for (CanalEntry.Column column : columns) {

            if(column.getIsKey())
            {
                sqlKey.append(column.getName() + " = \'\'" + column.getValue() + "\'\'");
            }
            else
            {
                sqlColumn.append(column.getName() + " =\'\'" + column.getValue() + "\'\'");
            }

            if(index > 1){
                sqlColumn.append(',');
            }
            index++;
        }

        sql.append(" UPDATE   `"+ dbName + "`.`"+ tableName + "`  ");
        if(columns.size()>0) {
            sql.append(" SET " + sqlColumn.toString());
            sql.append(" WHERE " + sqlKey.toString());
            // System.out.println(sql.toString());
            Exec(GenInsertSQL(tableName, sql.toString()));
        }else{
            System.out.println("Statement Empty!");
        }
    }

    public static void Insert(CanalEntry.Entry entry, List<CanalEntry.Column> columns)
    {
        String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlColumn = new StringBuilder();
        StringBuilder sqlValue = new StringBuilder();
        sql.append("INSERT INTO `"+ dbName + "`.`"+ tableName + "` (");
        int index = 0;
        for (CanalEntry.Column column : columns) {
            if(index > 0){
                sqlColumn.append(',');
                sqlValue.append(',');
            }
            sqlColumn.append(column.getName());
            sqlValue.append("\'\'" + column.getValue() + "\'\'");
            index++;
        }
        sql.append(sqlColumn.toString() + " ) VALUES(" + sqlValue.toString() +");");
        System.out.println(sql.toString());
        if(columns.size()>0) {
            Exec(GenInsertSQL(tableName, sql.toString()));
        }
    }

    public static void DropTable(CanalEntry.Entry entry, String sqlStatement)
    {
        // String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        Exec(GenInsertSQL(tableName, sqlStatement));
    }

    public static void CreateTable(CanalEntry.Entry entry, String sqlStatement)
    {
        // String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        Exec(GenInsertSQL(tableName, sqlStatement));
    }

    public static void AlterTable(CanalEntry.Entry entry, String sqlStatement)
    {
        // String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        Exec(GenInsertSQL(tableName, sqlStatement));
    }

    private static String GenInsertSQL(String tableName, String sqlStatement ){

        String sql = "  INSERT INTO [TableSchemaChange]\n" +
                "           ([TableName]\n" +
                "           ,[Scripts]\n" +
                "           ,[CreatedAt])\n" +
                "     VALUES\n" +
                "           ('" + tableName + "'\n" +
                "           ,'" + sqlStatement  + "'\n" +
                "           , GETDATE());";

        return sql;
    }
    private static void Exec(String sqlStatement)
    {
        System.out.println(sqlStatement);
        try {
            connection = DriverManager.getConnection(connectionString);
            statement = connection.createStatement();
            statement.execute(sqlStatement);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // Close the connections after the data has been handled.
            if (statement != null) try { statement.close(); } catch(Exception e) {}
            if (connection != null) try { connection.close(); } catch(Exception e) {}
        }
    }
}
