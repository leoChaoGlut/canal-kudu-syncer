package personal.leo.cks.server.util;

import java.util.concurrent.ConcurrentHashMap;

public class PropsHolder {
    private final static ConcurrentHashMap<String, String> props = new ConcurrentHashMap<>();

    public static String get(String key) {
        return props.get(key);
    }

    public static String put(String key, String value) {
        return props.put(key, value);
    }
}
