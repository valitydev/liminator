package com.empayre.liminator.handler.impl;

import com.empayre.liminator.handler.Handler;
import com.empayre.liminator.model.LimitValue;
import com.empayre.liminator.service.LimitOperationsHistoryService;
import dev.vality.liminator.LimitResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class GetLastLimitsValuesHandler implements Handler<List<String>, List<LimitResponse>> {

    private final LimitOperationsHistoryService limitOperationsHistoryService;
    private final Converter<List<LimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponseConverter;

    @Transactional
    @Override
    public List<LimitResponse> handle(List<String> limitIdNames) throws TException {
        List<LimitValue> currentLimitValues = limitOperationsHistoryService.getCurrentLimitValue(limitIdNames);
        log.debug("Success get last limits: {}", Arrays.toString(currentLimitValues.toArray()));
        return currentLimitValuesToLimitResponseConverter.convert(currentLimitValues);
    }
}
