package com.empayre.liminator.handler.impl;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.handler.FinalizeOperationHandler;
import com.empayre.liminator.service.LimitDataService;
import com.empayre.liminator.service.LimitOperationsHistoryService;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationNotFound;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.empayre.liminator.domain.enums.OperationState.COMMIT;
import static com.empayre.liminator.domain.enums.OperationState.ROLLBACK;

@Slf4j
@Component
@RequiredArgsConstructor
public class FinalizeOperationHandlerImpl implements FinalizeOperationHandler {

    private final LimitDataService limitDataService;
    private final LimitOperationsHistoryService limitOperationsHistoryService;

    @Transactional
    @Override
    public void handle(LimitRequest request, OperationState state) throws TException {
        List<LimitData> limitData = limitDataService.get(request, state.getLiteral());
        checkExistedHoldOperations(request, limitData, state);
        if (!List.of(COMMIT, ROLLBACK).contains(state)) {
            throw new TException();
        }
        int[] counts = limitOperationsHistoryService.writeOperations(request, state);
        checkUpdatedOperationsConsistency(request, state, counts.length);
    }

    private void checkExistedHoldOperations(LimitRequest request,
                                            List<LimitData> limitData,
                                            OperationState state) throws TException {
        String logPrefix = state.getLiteral();
        String operationId = request.getOperationId();
        var limitNames = limitData.stream()
                .map(LimitData::getName)
                .toList();
        List<OperationState> operationFinalStates = List.of(COMMIT, ROLLBACK);
        var existedFinaleOperations = limitOperationsHistoryService.get(operationId, limitNames, operationFinalStates);
        if (limitNames.size() == existedFinaleOperations.size()) {
            log.error("[{}] Existed hold operations with ID {} not found (request: {})",
                    logPrefix, operationId, request);
            throw new OperationNotFound();
        }
    }

    private void checkUpdatedOperationsConsistency(LimitRequest request,
                                                   OperationState state,
                                                   int updatedRowsCount) throws TException {
        int changesSize = request.getLimitChanges().size();
        if (updatedRowsCount != changesSize) {
            log.error("[{}] Count of updated rows ({}) is not equal to the expected count of updated operations " +
                            "(rollback size: {}, request: {})",
                    state.getLiteral(), updatedRowsCount, changesSize, request);
            throw new OperationNotFound();
        }
    }
}
