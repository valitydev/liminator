package com.empayre.liminator.handler.impl;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.handler.FinalizeOperationHandler;
import com.empayre.liminator.service.LimitDataService;
import com.empayre.liminator.service.LimitOperationsLoggingService;
import dev.vality.liminator.LimitRequest;
import dev.vality.liminator.OperationNotFound;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class FinalizeOperationHandlerImpl implements FinalizeOperationHandler {

    private final OperationDao operationDao;
    private final LimitDataService limitDataService;
    private final LimitOperationsLoggingService limitOperationsLoggingService;

    @Transactional
    @Override
    public void handle(LimitRequest request, OperationState state) throws TException {
        List<LimitData> limitData = limitDataService.get(request, state.getLiteral());
        checkExistedHoldOperations(request, limitData, state);
        List<Long> limitIds = limitData.stream()
                .map(LimitData::getId)
                .toList();
        int updatedRowsCount = switch (state) {
            case COMMIT -> operationDao.commit(request.getOperationId(), limitIds);
            case ROLLBACK -> operationDao.rollback(request.getOperationId(), limitIds);
            default -> throw new TException();
        };

        checkUpdatedOperationsConsistency(request, state, updatedRowsCount);
        limitOperationsLoggingService.writeOperations(request, state);
    }

    private void checkExistedHoldOperations(LimitRequest request,
                                            List<LimitData> limitData,
                                            OperationState state) throws TException {
        String logPrefix = state.getLiteral();
        String operationId = request.getOperationId();
        List<Long> limitIds = limitData.stream()
                .map(LimitData::getId)
                .toList();
        List<Operation> existedHoldOperations = operationDao.get(operationId, limitIds, List.of(OperationState.HOLD));
        if (CollectionUtils.isEmpty(existedHoldOperations)) {
            log.error("[{}] Existed hold operations with ID {} not found: {} (request: {})",
                    logPrefix, operationId, existedHoldOperations, request);
            throw new OperationNotFound();
        }
        if (limitIds.size() != existedHoldOperations.size()) {
            log.error("[{}] Count of existed hold operations for limits is not equal to expected (existed size: {}, " +
                            "expected size: {}, request: {})", logPrefix, existedHoldOperations.size(),
                    limitIds.size(), request);
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
