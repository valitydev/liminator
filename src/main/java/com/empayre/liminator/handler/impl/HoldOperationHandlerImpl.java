package com.empayre.liminator.handler.impl;

import com.empayre.liminator.converter.OperationConverter;
import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.handler.HoldOperationHandler;
import com.empayre.liminator.service.LimitDataGettingService;
import com.empayre.liminator.service.LimitOperationsLoggingService;
import com.empayre.liminator.util.LimitDataUtils;
import dev.vality.liminator.DuplicateOperation;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationAlreadyInFinalState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class HoldOperationHandlerImpl implements HoldOperationHandler {

    private final OperationDao operationDao;
    private final LimitDataGettingService limitDataGettingService;
    private final OperationConverter operationConverter;
    private final LimitOperationsLoggingService limitOperationsLoggingService;

    private static final String LOG_PREFIX = "HOLD";

    @Transactional
    @Override
    public void handle(LimitRequest request) throws TException {
        List<LimitData> limitData = limitDataGettingService.get(request, LOG_PREFIX);
        Map<String, Long> limitNamesMap = LimitDataUtils.createLimitNamesMap(limitData);

        checkExistedHoldOperations(limitNamesMap, request.getOperationId());
        checkExistedFinalizeOperations(limitNamesMap, request.getOperationId());

        operationDao.saveBatch(convertToOperation(request, limitNamesMap));
        limitOperationsLoggingService.writeOperations(request, OperationState.HOLD);
    }

    private List<Operation> convertToOperation(LimitRequest request, Map<String, Long> limitNamesMap) {
        return request.getLimitNames().stream()
                .map(limitName -> operationConverter.convert(request, limitNamesMap.get(limitName)))
                .toList();
    }

    private void checkExistedHoldOperations(Map<String, Long> limitNamesMap, String operationId) throws TException {
        List<Operation> existedHoldOperations =
                operationDao.get(operationId, limitNamesMap.values(), List.of(OperationState.HOLD));
        if (!CollectionUtils.isEmpty(existedHoldOperations)) {
            log.error("[{}] DB already has hold operation {}: {}", LOG_PREFIX, operationId, existedHoldOperations);
            throw new DuplicateOperation();
        }
    }

    private void checkExistedFinalizeOperations(Map<String, Long> limitNamesMap, String operationId) throws TException {
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
    }
}
