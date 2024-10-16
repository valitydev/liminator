package com.empayre.liminator.converter;

import com.empayre.liminator.model.LimitValue;
import dev.vality.liminator.LimitResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

@Slf4j
@Component
public class LimitValuesByLimitNameToLimitResponseConverter
        implements Converter<Map.Entry<String, List<LimitValue>>, LimitResponse> {

    @Override
    public LimitResponse convert(Map.Entry<String, List<LimitValue>> source) {
        LimitResponse limitResponse = new LimitResponse();
        limitResponse.setLimitName(source.getKey());
        limitResponse.setLimitId(source.getValue().get(0).getLimitId());
        long commitValue = 0L;
        long totalValue = 0L;
        for (LimitValue limitValue : source.getValue()) {
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
    }
}
