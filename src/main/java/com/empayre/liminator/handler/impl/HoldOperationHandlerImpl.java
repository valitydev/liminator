package com.empayre.liminator.handler.impl;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.handler.HoldOperationHandler;
import com.empayre.liminator.service.LimitDataService;
import com.empayre.liminator.service.LimitOperationsHistoryService;
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

import java.util.ArrayList;
import java.util.List;

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
        var limitNames = new ArrayList<String>();
        List<LimitChange> limitChanges = request.getLimitChanges();
        for (LimitChange change : limitChanges) {
            LimitData limitData = limitDataService.get(change.getLimitName());
            if (limitData != null) {
                limitNames.add(limitData.getName());
            } else {
                limitDataService.save(change);
                limitNames.add(change.getLimitName());
            }
        }
        String operationId = request.getOperationId();
        checkExistedFinalizeOperations(limitNames, operationId);
        log.info("Save operation: {} with limits: {}", operationId, limitNames);
        int[] counts = limitOperationsHistoryService.writeOperations(request, OperationState.HOLD);
        log.info("Success saved operation: {} with {} limits", operationId, counts.length);
    }

    private void checkExistedFinalizeOperations(List<String> limitNames,
                                                String operationId) throws TException {
        var existedFinalizeOperations = limitOperationsHistoryService.get(
                operationId,
                limitNames,
                List.of(OperationState.COMMIT, OperationState.ROLLBACK)
        );
        if (!CollectionUtils.isEmpty(existedFinalizeOperations)) {
            log.error("[{}] DB already has commit/rollback operation {}: {}",
                    LOG_PREFIX, operationId, existedFinalizeOperations);
            throw new OperationAlreadyInFinalState();
        }
    }
}
