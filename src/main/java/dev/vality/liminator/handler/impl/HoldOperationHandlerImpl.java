package dev.vality.liminator.handler.impl;

import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.handler.HoldOperationHandler;
import dev.vality.liminator.service.LimitDataService;
import dev.vality.liminator.service.LimitOperationsHistoryService;
import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationAlreadyInFinalState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.HashMap;
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
        log.info("Save operation: {} with limits: {}", operationId, Arrays.toString(limitNamesMap.keySet().toArray()));
        int[] counts = limitOperationsHistoryService.writeOperations(request, OperationState.HOLD, limitNamesMap);
        log.info("Success saved operation: {} with {} limits", operationId, counts.length);
    }

    private void checkExistedFinalizeOperations(Map<String, Long> limitNamesMap,
                                                String operationId) throws TException {
        var existedFinalizeOperations = limitOperationsHistoryService.get(
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
