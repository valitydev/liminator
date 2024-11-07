package dev.vality.liminator.util;

import dev.vality.liminator.domain.tables.pojos.LimitData;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LimitDataUtils {

    public static Map<String, Long> createLimitNamesMap(List<LimitData> limitData) {
        Map<String, Long> map = new HashMap<>();
        for (LimitData data : limitData) {
            map.put(data.getName(), data.getId());
        }
        return map;
    }
}
