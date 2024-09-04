package com.empayre.liminator.handler.impl;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.handler.FinalizeOperationHandler;
import com.empayre.liminator.service.LimitDataGettingService;
import dev.vality.liminator.LimitRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class RollbackLimitValueHandler implements FinalizeOperationHandler<LimitRequest> {

    private final OperationDao operationDao;
    private final LimitDataGettingService limitDataGettingService;

    private static final String LOG_PREFIX = "ROLLBACK";

    @Override
    @Transactional
    public void handle(LimitRequest request) throws TException {
        limitDataGettingService.get(request, LOG_PREFIX);

        List<String> limitNames = request.getLimitNames();
        String operationId = request.getOperationId();
        int updatedRowsCount = operationDao.rollback(limitNames, operationId);
        if (updatedRowsCount != limitNames.size()) {
            log.error("[{}] Count of updated rows ({}) is not equal to the count of source rollback operations " +
                            "(operationId: {}, rollback size: {})",
                    LOG_PREFIX, updatedRowsCount, operationId, limitNames.size());
        }
    }
}
