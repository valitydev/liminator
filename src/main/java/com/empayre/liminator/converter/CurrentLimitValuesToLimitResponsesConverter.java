package com.empayre.liminator.converter;

import com.empayre.liminator.model.CurrentLimitValue;
import dev.vality.liminator.LimitResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class CurrentLimitValuesToLimitResponsesConverter
        implements Converter<List<CurrentLimitValue>, List<LimitResponse>> {


    @Override
    public List<LimitResponse> convert(List<CurrentLimitValue> values) {
        if (CollectionUtils.isEmpty(values)) {
            log.info("Received LimitValues array is empty");
            return new ArrayList<>();
        }
        return values.stream()
                .map(this::toLimitResponse)
                .toList();
    }

    private LimitResponse toLimitResponse(CurrentLimitValue value) {
        return new LimitResponse()
                .setLimitId(value.getLimitId())
                .setLimitName(value.getLimitName())
                .setCommitValue(value.getCommitValue())
                .setTotalValue(value.getHoldValue() + value.getCommitValue());
    }
}
