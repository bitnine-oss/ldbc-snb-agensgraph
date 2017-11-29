package net.bitnine.ldbcimpl.util;

import net.bitnine.agensgraph.util.Jsonb;
import net.bitnine.agensgraph.util.JsonbUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ktlee on 16. 10. 12.
 */
public class JsonArrayUtils {
    public static List<String> toStringList(Jsonb arr) {
        List<String> l = new ArrayList<>();
        for (int i = 0; i < arr.size(); ++i) {
            if (! arr.isNull(i))
                l.add(arr.getString(i));
        }
        return l;
    }

    public static List<List<Object>> toListofList(Jsonb arr) {
        List<List<Object>> ll = new ArrayList<>();
        for (int i = 0; i < arr.size(); ++i) {
            if (arr.isNull(i))
                continue;
            try {
                Jsonb prop = arr.getArray(i);
                List<Object> l = new ArrayList<>(3);
                l.add(prop.getString(0));
                l.add(Long.parseLong(prop.getString(1)));
                l.add(prop.getString(2));
                ll.add(l);
            } catch (Exception e) {
            }
        }
        return ll;
    }
    public static List<Long> toLongList(Jsonb arr) {
        List<Long> l = new ArrayList<>();
        for (int i = 0; i < arr.size(); ++i) {
            if (! arr.isNull(i))
                l.add(arr.getLong(i));
        }
        return l;
    }
}
