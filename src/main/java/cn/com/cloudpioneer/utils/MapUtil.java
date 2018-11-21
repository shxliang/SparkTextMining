package cn.com.cloudpioneer.utils;

import java.util.*;

/**
 *
 * @author ting
 * @date 22/06/2017
 */
public class MapUtil {
    /**
     * sort given map by values when value of map is comparable
     *
     * @param map given map
     * @param <K> key type of map
     * @param <V> value type of map, need to be comparable
     * @return map sorted w/ values
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                // add - for desc sort
                return -(o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }
}
