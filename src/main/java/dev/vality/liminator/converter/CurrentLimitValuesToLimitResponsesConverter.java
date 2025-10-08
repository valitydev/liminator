package dev.vality.liminator.converter;

import dev.vality.liminator.LimitResponse;
import dev.vality.liminator.model.CurrentLimitValue;
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
                .setTotalValue(calculateTotal(value));
    }

    private long calculateTotal(CurrentLimitValue value) {
        if (isZeroCommit(value)) {
            return 0;
        }
        return value.getHoldValue() + value.getCommitValue();
    }

    private boolean isZeroCommit(CurrentLimitValue value) {
        return value.getCommitCount() > 0 && value.getCommitValue() == 0;
    }
}
