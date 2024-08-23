package org.example.generics;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yuankuo.xia
 * @Date 2024/6/26
 */
public interface Method {

    <K,V> V get(K k, Map<K,V> map);

    class KK implements Method{
        Map<String, Object> map = new HashMap<>();
        @Override
        public <K,V> V get(K k, Map<K,V> map) {
            return map.get(k);
        }
    }

    static class Main{
        public static void main(String[] args){
            Method method = new KK();
            method.get("", new HashMap<String, String>());
        }
    }
}
