package org.example.connector.pool;

import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Deque;
import java.util.List;

/**
 * @author yuankuo.xia
 * @Date 2024/5/31
 */
public class ConnectionPoolImpl implements ConnectionPool<List<Tuple2<Connection, Integer>>> {

    private static volatile ConnectionPoolImpl pool;

    private Deque<List<Tuple2<Connection, Integer>>> free;

    public static ConnectionPoolImpl instance(){
        if(pool == null){
            synchronized (ConnectionPoolImpl.class) {
                if(pool == null){
                    pool = new ConnectionPoolImpl();
                }
            }
        }
        return pool;
    }

    private ConnectionPoolImpl(){

    }

    @Override
    public List<Tuple2<Connection, Integer>> getConnection() throws SQLException {
        return null;
    }

    @Override
    public void releaseConnection(List<Tuple2<Connection, Integer>> tuple2s) throws SQLException {
    }

    @Override
    public void shutdown() throws SQLException {

    }
}
