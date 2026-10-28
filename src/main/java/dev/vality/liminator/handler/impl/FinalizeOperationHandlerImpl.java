package dev.vality.liminator.handler.impl;

import dev.vality.liminator.InvalidRequest;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationNotFound;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.handler.FinalizeOperationHandler;
import dev.vality.liminator.model.PendingFinalization;
import dev.vality.liminator.service.LimitDataService;
import dev.vality.liminator.service.LimitOperationsHistoryService;
import dev.vality.liminator.util.LimitRequestUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.vality.liminator.domain.enums.OperationState.COMMIT;
import static dev.vality.liminator.domain.enums.OperationState.ROLLBACK;

@Slf4j
@Component
@RequiredArgsConstructor
public class FinalizeOperationHandlerImpl implements FinalizeOperationHandler {

    private final LimitDataService limitDataService;
    private final LimitOperationsHistoryService limitOperationsHistoryService;

    @Transactional(rollbackFor = TException.class)
    @Override
    public void handle(LimitRequest request, OperationState state) throws TException {
        validateFinalizeOperation(request, state);
        String operationId = request.getOperationId();
        limitOperationsHistoryService.lockOperation(operationId);
        Map<String, Long> limitNamesMap = getLimitDataMap(request, state.getLiteral());
        var finalOperations =
                limitOperationsHistoryService.getExistedFinalOperations(operationId, limitNamesMap.values());
        var finalOperationsByLimitId = checkFinalOperations(state, operationId, finalOperations);
        var pending = preparePendingFinalization(request, state, limitNamesMap, finalOperationsByLimitId);
        finalizePendingChanges(operationId, state, pending);
    }

    private void validateFinalizeOperation(LimitRequest request, OperationState state) throws TException {
        if (state != COMMIT && state != ROLLBACK) {
            throw new InvalidRequest();
        }
        if (LimitRequestUtils.hasDuplicateLimitNames(request)) {
            throw new InvalidRequest();
        }
    }

    private Map<String, Long> getLimitDataMap(LimitRequest request, String source) throws TException {
        var limitNamesMap = new HashMap<String, Long>();
        List<LimitData> limitDataList = limitDataService.get(request, source);
        for (LimitData limitData : limitDataList) {
            limitNamesMap.put(limitData.getName(), limitData.getId());
        }
        return limitNamesMap;
    }

    private PendingFinalization preparePendingFinalization(
            LimitRequest request,
            OperationState state,
            Map<String, Long> limitNamesMap,
            Map<Long, OperationStateHistory> finalOperationsByLimitId
    ) throws OperationNotFound {
        List<LimitChange> pendingChanges = new ArrayList<>();
        Map<String, Long> pendingLimitNamesMap = new HashMap<>();
        for (LimitChange change : request.getLimitChanges()) {
            Long limitId = limitNamesMap.get(change.getLimitName());
            OperationStateHistory finalOperation = finalOperationsByLimitId.get(limitId);
            if (finalOperation == null) {
                pendingChanges.add(change);
                pendingLimitNamesMap.put(change.getLimitName(), limitId);
            } else if (state == COMMIT) {
                limitOperationsHistoryService.checkNewCommitValueCorrectness(
                        request.getOperationId(), change, finalOperation.getOperationValue());
            }
        }
        return new PendingFinalization(pendingChanges, pendingLimitNamesMap);
    }

    private void finalizePendingChanges(String operationId,
                                        OperationState state,
                                        PendingFinalization pending) throws TException {
        if (pending.changes().isEmpty()) {
            return;
        }
        limitOperationsHistoryService.checkCorrectnessFinalizingOperation(
                operationId, pending.changes(), state, pending.limitNamesMap());
        int[] counts = limitOperationsHistoryService.writeOperations(
                operationId, pending.changes(), state, pending.limitNamesMap());
        checkUpdatedOperationsConsistency(operationId, state, pending.changes().size(), counts.length);
    }

    private Map<Long, OperationStateHistory> checkFinalOperations(
            OperationState state, String operationId, List<OperationStateHistory> finalOperations)
            throws OperationNotFound {
        Map<Long, OperationStateHistory> finalOperationsByLimitId = new HashMap<>();
        for (OperationStateHistory operation : finalOperations) {
            if (operation.getState() != state
                    || finalOperationsByLimitId.putIfAbsent(operation.getLimitDataId(), operation) != null) {
                log.error("[{}] Conflicting final history for operation {}: {}",
                        state.getLiteral(), operationId, finalOperations);
                throw new OperationNotFound();
            }
        }
        return finalOperationsByLimitId;
    }

    private void checkUpdatedOperationsConsistency(String operationId,
                                                   OperationState state,
                                                   int changesSize,
                                                   int updatedRowsCount) throws TException {
        if (updatedRowsCount != changesSize) {
            log.error("[{}] Count of updated rows ({}) is not equal to the expected count of updated operations " +
                            "(changes size: {}, operationId: {})",
                    state.getLiteral(), updatedRowsCount, changesSize, operationId);
            throw new OperationNotFound();
        }
    }
}
