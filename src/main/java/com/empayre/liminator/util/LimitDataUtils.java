package com.empayre.liminator.util;

import com.empayre.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.LimitRequest;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LimitDataUtils {

    public static List<String> getLimitNames(List<LimitRequest> requestList) {
        return requestList.stream()
                .map(LimitRequest::getLimitName)
                .distinct()
                .toList();
    }

    public static Map<String, Long> createLimitNamesMap(List<LimitData> limitData) {
        Map<String, Long> map = new HashMap<>();
        for (LimitData data : limitData) {
            map.put(data.getName(), data.getId());
        }
        return map;
    }
}
