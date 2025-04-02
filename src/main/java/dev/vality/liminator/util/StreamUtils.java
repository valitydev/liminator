package dev.vality.liminator.util;

import lombok.experimental.UtilityClass;

import java.util.Map;

import static java.util.stream.Collectors.toMap;

@UtilityClass
public class StreamUtils {

    public static Map<Long, String> getInversedMap(Map<String, Long> map) {
        return map.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
    }
}
