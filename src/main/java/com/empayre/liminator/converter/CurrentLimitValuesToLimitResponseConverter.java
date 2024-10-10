package com.empayre.liminator.converter;

import com.empayre.liminator.model.LimitValue;
import dev.vality.liminator.LimitResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class CurrentLimitValuesToLimitResponseConverter implements Converter<List<LimitValue>, List<LimitResponse>> {

    @Override
    public List<LimitResponse> convert(List<LimitValue> values) {
        if (values == null) {
            log.info("Received LimitValues array is empty");
            return new ArrayList<>();
        }
        return values.stream()
                .map(limitValue -> new LimitResponse(
                        limitValue.getLimitName(),
                        limitValue.getCommitValue(),
                        getTotalValue(limitValue))
                        .setLimitId(limitValue.getLimitId())
                )
                .toList();
    }

    private static long getTotalValue(LimitValue limitValue) {
        return limitValue.getHoldValue() + limitValue.getCommitValue() - limitValue.getRollbackValue();
    }
}
