package org.example.exception;

/**
 * @author yuankuo.xia
 * @Date 2024/5/29
 */
public class InterruptSqlException extends Exception{
    public InterruptSqlException(Exception e){
        super(e);
    }

    public InterruptSqlException(){
        super();
    }
}
