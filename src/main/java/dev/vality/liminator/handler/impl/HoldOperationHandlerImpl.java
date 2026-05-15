package dev.vality.liminator.handler.impl;

import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationAlreadyInFinalState;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.handler.HoldOperationHandler;
import dev.vality.liminator.service.LimitDataService;
import dev.vality.liminator.service.LimitOperationsHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@Transactional
@RequiredArgsConstructor
public class HoldOperationHandlerImpl implements HoldOperationHandler {

    private final LimitOperationsHistoryService limitOperationsHistoryService;
    private final LimitDataService limitDataService;

    private static final String LOG_PREFIX = "HOLD";
    private static final List<OperationState> FINAL_STATES = List.of(OperationState.COMMIT, OperationState.ROLLBACK);

    @Transactional
    @Override
    public void handle(LimitRequest request) throws TException {
        // One query to get limitNames or save it if not exist
        Map<String, Long> limitNamesMap = limitDataService.getOrCreateLimitDataMap(request.getLimitChanges());
        String operationId = request.getOperationId();

        // One history query to get states and then filter
        List<OperationStateHistory> existedOperations = limitOperationsHistoryService.get(
                operationId,
                limitNamesMap.values(),
                List.of(OperationState.HOLD, OperationState.COMMIT, OperationState.ROLLBACK)
        );
        checkExistedFinalizeOperations(operationId, existedOperations);
        if (isAlreadyExistHoldOperations(operationId, existedOperations)) {
            return;
        }
        log.info("Save operation: {} with limits: {}", operationId, Arrays.toString(limitNamesMap.keySet().toArray()));
        int[] counts = limitOperationsHistoryService.writeOperations(request, OperationState.HOLD, limitNamesMap);
        log.info("Success saved operation: {} with {} limits", operationId, counts.length);
    }

    private boolean isAlreadyExistHoldOperations(String operationId,
                                                 List<OperationStateHistory> existedOperations) {
        List<OperationStateHistory> existedHoldOperations = existedOperations.stream()
                .filter(operation -> operation.getState() == OperationState.HOLD)
                .toList();
        if (CollectionUtils.isEmpty(existedHoldOperations)) {
            return false;
        }
        log.warn("[{}] DB already has operation with id {}: {}",
                LOG_PREFIX, operationId, existedHoldOperations);
        return true;
    }

    private void checkExistedFinalizeOperations(String operationId,
                                                List<OperationStateHistory> existedOperations) throws TException {
        List<OperationStateHistory> existedFinalizeOperations = existedOperations.stream()
                .filter(operation -> FINAL_STATES.contains(operation.getState()))
                .toList();
        if (!CollectionUtils.isEmpty(existedFinalizeOperations)) {
            log.error("[{}] DB already has commit/rollback operation {}: {}",
                    LOG_PREFIX, operationId, existedFinalizeOperations);
            throw new OperationAlreadyInFinalState();
        }
    }
}
