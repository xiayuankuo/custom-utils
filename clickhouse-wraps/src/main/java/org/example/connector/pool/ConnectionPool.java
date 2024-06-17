package org.example.connector.pool;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author yuankuo.xia
 * @Date 2024/5/31
 */
public interface ConnectionPool<T> {
    T getConnection() throws SQLException;
    void releaseConnection(T t) throws SQLException;
    void shutdown() throws SQLException;
}
