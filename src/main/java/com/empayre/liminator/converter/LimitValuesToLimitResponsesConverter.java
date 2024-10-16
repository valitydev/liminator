package com.empayre.liminator.converter;

import com.empayre.liminator.model.LimitValue;
import dev.vality.liminator.LimitResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

@Slf4j
@Component
@RequiredArgsConstructor
public class LimitValuesToLimitResponsesConverter implements Converter<List<LimitValue>, List<LimitResponse>> {

    private final Converter<Map.Entry<String, List<LimitValue>>, LimitResponse> limitResponseConverter;

    @Override
    public List<LimitResponse> convert(List<LimitValue> values) {
        if (CollectionUtils.isEmpty(values)) {
            log.info("Received LimitValues array is empty");
            return new ArrayList<>();
        }
        Map<String, List<LimitValue>> valuesPerLimitName = values.stream()
                .collect(groupingBy(LimitValue::getLimitName));
        return valuesPerLimitName.entrySet().stream()
                .map(limitResponseConverter::convert)
                .toList();
    }
}
