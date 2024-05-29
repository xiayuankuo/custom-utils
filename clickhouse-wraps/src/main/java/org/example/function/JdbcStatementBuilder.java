package org.example.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yuankuo.xia
 * @Date 2024/5/27
 */
public interface JdbcStatementBuilder {
    Logger LOG = LoggerFactory.getLogger(JdbcStatementBuilder.class);
    static void statementSetting(PreparedStatement statement, Object value, Integer index) throws SQLException{
        if(value instanceof String){
            statement.setString(index, value.toString());
        } else if(value instanceof Integer){
            statement.setInt(index, (Integer) value);
        } else if(value instanceof Long){
            statement.setLong(index, (Long) value);
        }if(value instanceof Boolean){
            statement.setBoolean(index, (Boolean) value);
        } else if(value instanceof Float){
            statement.setFloat(index, (Float) value);
        } else if(value instanceof Double){
            statement.setDouble(index, (Double) value);
        } else if(value instanceof Byte){
            statement.setByte(index, (Byte) value);
        } else if(value instanceof Short){
            statement.setShort(index, (Short) value);
        }else {
            statement.setString(index, value.toString());
            LOG.error("Unsupported type, use string! {}", value.toString());
        }
    }
}
