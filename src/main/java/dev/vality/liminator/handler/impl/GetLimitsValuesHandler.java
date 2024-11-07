package dev.vality.liminator.handler.impl;

import dev.vality.liminator.handler.Handler;
import dev.vality.liminator.model.CurrentLimitValue;
import dev.vality.liminator.service.LimitOperationsHistoryService;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
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
public class GetLimitsValuesHandler implements Handler<LimitRequest, List<LimitResponse>> {

    private final LimitOperationsHistoryService limitOperationsHistoryService;
    private final Converter<List<CurrentLimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponsesConverter;

    @Transactional
    @Override
    public List<LimitResponse> handle(LimitRequest request) throws TException {
        List<String> limitNames = request.getLimitChanges().stream()
                .map(LimitChange::getLimitName)
                .toList();
        String operationId = request.getOperationId();
        List<CurrentLimitValue> limitValues =
                limitOperationsHistoryService.getCurrentLimitValue(limitNames, operationId);
        log.debug("Success get limits: {}", Arrays.toString(limitValues.toArray()));
        return currentLimitValuesToLimitResponsesConverter.convert(limitValues);
    }
}
