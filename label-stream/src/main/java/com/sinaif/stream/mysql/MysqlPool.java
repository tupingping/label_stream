package com.sinaif.stream.mysql;

import com.sinaif.stream.common.utils.ConfigRespostory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class MysqlPool {
    private static HikariDataSource DATA_SOURCE;

    static {
        init();
    }

    private static void init(){
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(ConfigRespostory.value("jdbc.url"));
        config.setUsername(ConfigRespostory.value("user.name"));
        config.setPassword(ConfigRespostory.value("user.password"));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        DATA_SOURCE = new HikariDataSource(config);
    }

    public static HikariDataSource getDataSource(){
        return DATA_SOURCE;
    }

    public static ResultSet simpleQuery(String sql){
        try{
            Connection connection = DATA_SOURCE.getConnection();
            Statement statement = connection.createStatement();
            return statement.executeQuery(sql);

        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }
        return null;
    }
}
