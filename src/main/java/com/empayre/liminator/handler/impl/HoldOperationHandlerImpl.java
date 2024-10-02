package com.empayre.liminator.handler.impl;

import com.empayre.liminator.converter.OperationConverter;
import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.handler.HoldOperationHandler;
import com.empayre.liminator.service.LimitDataService;
import com.empayre.liminator.service.LimitOperationsLoggingService;
import dev.vality.liminator.DuplicateOperation;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationAlreadyInFinalState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class HoldOperationHandlerImpl implements HoldOperationHandler {

    private final OperationDao operationDao;
    private final OperationConverter operationConverter;
    private final LimitOperationsLoggingService limitOperationsLoggingService;
    private final LimitDataService limitDataService;

    @Value("${service.skipExistedHoldOps}")
    private boolean skipExistedHoldOps;

    private static final String LOG_PREFIX = "HOLD";

    @Transactional
    @Override
    public void handle(LimitRequest request) throws TException {
        var limitNamesMap = new HashMap<String, Long>();
        List<LimitChange> limitChanges = request.getLimitChanges();
        for (LimitChange change : limitChanges) {
            LimitData limitData = limitDataService.get(change.getLimitName());
            if (limitData != null) {
                limitNamesMap.put(limitData.getName(), limitData.getId());
            } else {
                var limitId = limitDataService.save(change);
                limitNamesMap.put(change.getLimitName(), limitId);
            }
        }
        String operationId = request.getOperationId();
        checkExistedFinalizeOperations(limitNamesMap, operationId);
        if (!skipExistedHoldOps) {
            log.debug("Skip check existed hold operation for operationId: {}", operationId);
            checkExistedHoldOperations(limitNamesMap, operationId);
        }
        log.info("Save operation: {} with limits: {}", operationId, Arrays.toString(limitNamesMap.keySet().toArray()));
        operationDao.saveBatch(convertToOperation(request, limitNamesMap));
        limitOperationsLoggingService.writeOperations(request, OperationState.HOLD);
    }

    private List<Operation> convertToOperation(LimitRequest request, Map<String, Long> limitNamesMap) {
        var createdAt = LocalDateTime.now();
        return request.getLimitChanges().stream()
                .map(change -> operationConverter.convert(
                                request,
                                limitNamesMap.get(change.getLimitName()),
                                change.getValue(),
                                createdAt
                        )
                )
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

    private void checkExistedFinalizeOperations(Map<String, Long> limitNamesMap,
                                                String operationId) throws TException {
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
