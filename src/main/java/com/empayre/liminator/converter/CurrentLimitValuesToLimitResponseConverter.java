package com.empayre.liminator.converter;

import com.empayre.liminator.model.LimitValue;
import dev.vality.liminator.LimitResponse;
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
public class CurrentLimitValuesToLimitResponseConverter implements Converter<List<LimitValue>, List<LimitResponse>> {

    @Override
    public List<LimitResponse> convert(List<LimitValue> values) {
        if (CollectionUtils.isEmpty(values)) {
            log.info("Received LimitValues array is empty");
            return new ArrayList<>();
        }
        return toResponseList(values);
    }

    private List<LimitResponse> toResponseList(List<LimitValue> values) {
        Map<String, List<LimitValue>> valuesPerLimitName = values.stream()
                .collect(groupingBy(LimitValue::getLimitName));

        return valuesPerLimitName.entrySet().stream()
                .map(entry -> {
                    LimitResponse limitResponse = new LimitResponse();
                    limitResponse.setLimitName(entry.getKey());
                    limitResponse.setLimitId(entry.getValue().get(0).getLimitId());
                    long commitValue = 0L;
                    long totalValue = 0L;
                    for (LimitValue limitValue : entry.getValue()) {
                        switch (limitValue.getState()) {
                            case HOLD -> totalValue = totalValue + limitValue.getOperationValue();
                            case COMMIT -> {
                                commitValue = commitValue + limitValue.getOperationValue();
                                totalValue = totalValue - limitValue.getOperationValue();
                            }
                            case ROLLBACK -> totalValue = totalValue - limitValue.getOperationValue();
                            default -> throw new IllegalStateException("Unexpected value: " + limitValue.getState());
                        }
                    }
                    limitResponse.setCommitValue(commitValue);
                    limitResponse.setTotalValue(totalValue);
                    return limitResponse;
                })
                .toList();
    }
}
