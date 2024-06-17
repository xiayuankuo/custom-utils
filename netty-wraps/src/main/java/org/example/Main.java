package org.example;

import java.lang.reflect.Array;
import java.util.*;

/**
 * @author yuankuo.xia
 * @Date 2024/6/11
 */
public class Main {
    public static void main(String[] args) {
        Main m = new Main();
        m.findSubstring("11wordgoodgoodwordgoodbestword", new String[]{"word","good","best","word"});
    }

    public List<Integer> findSubstring(String s, String[] words) {
        Map<String, Integer> map2 = new HashMap<>(words.length*2);
        for(String ss : words){
            if(map2.containsKey(ss)){
                map2.put(ss, map2.get(ss)+1);
            } else {
                map2.put(ss, 1);
            }
        }


        List<Integer> list = new ArrayList<>();
        if(s.length() > 0 && words.length > 0) {

            for(int i=0; i<words[0].length(); i++){
                



            }


            int len = words.length * words[0].length();
            if(len <= s.length()){
                int sta =0;
                int end = len;
                while (end <= s.length()){
                    Map<String, Integer> map1 = new HashMap<>(words.length*2);
                    String str = s.substring(sta, end);
                    for(int i=0; i<str.length(); i+=words[0].length()){
                        String ss = str.substring(i, i+words[0].length());
                        if(map1.containsKey(ss)){
                            map1.put(ss, map1.get(ss)+1);
                        } else {
                            map1.put(ss, 1);
                        }
                    }

                    if(map1.equals(map2)) {
                        list.add(sta);

                    }

                    sta++;
                    end++;
                }
            }
        }
        return list;
    }
}
