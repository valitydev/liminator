package com.empayre.liminator.handler.impl;

import com.empayre.liminator.converter.OperationConverter;
import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.handler.Handler;
import com.empayre.liminator.model.LimitValue;
import com.empayre.liminator.service.LimitDataGettingService;
import com.empayre.liminator.util.LimitDataUtils;
import dev.vality.liminator.DuplicateOperation;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.LimitResponse;
import dev.vality.liminator.OperationAlreadyInFinalState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HoldLimitValueHandler implements Handler<LimitRequest, List<LimitResponse>> {

    private final OperationDao operationDao;
    private final LimitDataGettingService limitDataGettingService;
    private final Converter<List<LimitValue>, List<LimitResponse>> currentLimitValuesToLimitResponseConverter;
    private final OperationConverter operationConverter;

    private static final String LOG_PREFIX = "HOLD";

    @Transactional
    @Override
    public List<LimitResponse> handle(LimitRequest request) throws TException {
        if (request == null || CollectionUtils.isEmpty(request.getLimitNames())) {
            log.warn("[{}] LimitRequest or LimitNames is empty. Request: {}", LOG_PREFIX, request);
            return new ArrayList<>();
        }
        List<LimitData> limitData = limitDataGettingService.get(request, LOG_PREFIX);
        Map<String, Long> limitNamesMap = LimitDataUtils.createLimitNamesMap(limitData);
        String operationId = request.getOperationId();

        List<Operation> existedHoldOperations =
                operationDao.get(operationId, limitNamesMap.values(), List.of(OperationState.HOLD));
        if (!CollectionUtils.isEmpty(existedHoldOperations)) {
            log.error("[{}] DB already has hold operation {}: {}", LOG_PREFIX, operationId, existedHoldOperations);
            throw new DuplicateOperation();
        }

        List<Operation> existedFinalizeOperations = operationDao.get(
                operationId,
                limitNamesMap.values(),
                List.of(OperationState.COMMIT, OperationState.ROLLBACK)
        );
        if (!CollectionUtils.isEmpty(existedFinalizeOperations)) {
            log.error("[{}] DB already has commit/rollback operation {}: {}",
                    LOG_PREFIX, operationId, existedFinalizeOperations);
            throw new OperationAlreadyInFinalState();
        }

        List<Operation> operations = convertToOperation(request, limitNamesMap);
        operationDao.saveBatch(operations);

        List<String> limitNames = request.getLimitNames();
        return currentLimitValuesToLimitResponseConverter.convert(operationDao.getCurrentLimitValue(limitNames));
    }

    private List<Operation> convertToOperation(LimitRequest request, Map<String, Long> limitNamesMap) {
        return request.getLimitNames().stream()
                .map(limitName -> operationConverter.convert(request, limitNamesMap.get(limitName)))
                .toList();
    }
}
