package net.bitnine.ldbcimpl.util;

import net.bitnine.agensgraph.graph.property.JsonArray;
import net.bitnine.agensgraph.graph.property.JsonType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ktlee on 16. 10. 12.
 */
public class JsonArrayUtils {
    public static List<String> toStringList(JsonArray arr) {
        if (arr == null || arr.getJsonType() != JsonType.ARRAY)
            return null;
        List<String> l = new ArrayList<>();
        int size = arr.getJsonArray().size();
        for (int i = 0; i < size; ++i) {
            l.add(arr.getString(i));
        }
        return l;
    }
    public static List<Long> toLongList(JsonArray arr) {
        if (arr == null || arr.getJsonType() != JsonType.ARRAY)
            return null;
        List<Long> l = new ArrayList<>();
        int size = arr.getJsonArray().size();
        for (int i = 0; i < size; ++i) {
            l.add(arr.getLong(i));
        }
        return l;
    }
}
