package org.exmaple.design.pattern.singleton;

/**
 * @author yuankuo.xia
 * @Date 2024/7/12
 */
public class StaticSingleton {

    private StaticSingleton(){}

    public static StaticSingleton getInstance(){
        return Singleton.single;
    }

    public static class Singleton{
        private static final StaticSingleton single = new StaticSingleton();
    }
}
