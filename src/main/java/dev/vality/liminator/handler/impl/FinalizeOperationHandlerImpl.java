package dev.vality.liminator.handler.impl;

import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import dev.vality.liminator.handler.FinalizeOperationHandler;
import dev.vality.liminator.service.LimitDataService;
import dev.vality.liminator.service.LimitOperationsHistoryService;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationNotFound;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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

    @Transactional
    @Override
    public void handle(LimitRequest request, OperationState state) throws TException {
        Map<String, Long> limitNamesMap = getLimitDataMap(request, state.getLiteral());
        limitOperationsHistoryService.checkCorrectnessFinalizingOperation(request, limitNamesMap, state);
        if (!List.of(COMMIT, ROLLBACK).contains(state)) {
            throw new TException();
        }
        int[] counts = limitOperationsHistoryService.writeOperations(request, state, limitNamesMap);
        checkUpdatedOperationsConsistency(request, state, counts.length);
    }

    private HashMap<String, Long> getLimitDataMap(LimitRequest request, String source) throws TException {
        var limitNamesMap = new HashMap<String, Long>();
        List<LimitData> limitDataList = limitDataService.get(request, source);
        for (LimitData limitData : limitDataList) {
            limitNamesMap.put(limitData.getName(), limitData.getId());
        }
        return limitNamesMap;
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
