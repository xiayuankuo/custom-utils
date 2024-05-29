package org.example.function;

/**
 * @author yuankuo.xia
 * @Date 2024/5/28
 */
@FunctionalInterface
public interface ThrowingFunction<K, V, E extends Exception> {
    V apply(K t) throws E;
}
