package com.empayre.liminator.handler.impl;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.handler.FinalizeOperationHandler;
import com.empayre.liminator.service.LimitDataGettingService;
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
    private final LimitDataGettingService limitDataGettingService;

    private static final String LOG_PREFIX = "ROLLBACK";

    @Override
    @Transactional
    public void handle(LimitRequest request, OperationState state) throws TException {
        List<LimitData> limitData = limitDataGettingService.get(request, LOG_PREFIX);
        List<Long> limitIds = limitData.stream()
                .map(LimitData::getId)
                .toList();
        String operationId = request.getOperationId();
        List<Operation> existedHoldOperations = operationDao.get(operationId, limitIds, List.of(OperationState.HOLD));
        if (CollectionUtils.isEmpty(existedHoldOperations)) {
            log.error("[{}] Existed hold operations with ID {} not found: {} (request: {})",
                    LOG_PREFIX, operationId, existedHoldOperations, request);
            throw new OperationNotFound();
        }
        if (limitIds.size() != existedHoldOperations.size()) {
            log.error("[{}] Count of existed hold operations for limits is not equal to expected (existed size: {}, " +
                            "expected size: {}, request: {})", LOG_PREFIX, existedHoldOperations.size(),
                    limitIds.size(), request);
            throw new OperationNotFound();
        }
        int updatedRowsCount = switch (state) {
            case COMMIT -> operationDao.commit(request.getLimitNames(), operationId);
            case ROLLBACK -> operationDao.rollback(request.getLimitNames(), operationId);
            default -> throw new TException();
        };

        List<String> limitNames = request.getLimitNames();
        if (updatedRowsCount != limitNames.size()) {
            log.error("[{}] Count of updated rows ({}) is not equal to the expected count of updated operations " +
                            "(rollback size: {})",
                    LOG_PREFIX, updatedRowsCount, limitNames.size(), request);
            throw new OperationNotFound();
        }
    }
}
