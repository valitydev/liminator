package com.empayre.liminator.handler;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.service.DataConsistencyCheckingService;
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
public class RollbackLimitAmountHandler implements Handler<List<LimitRequest>, Boolean> {

    private final OperationDao operationDao;
    private final DataConsistencyCheckingService dataConsistencyCheckingService;

    private static final String LOG_PREFIX = "ROLLBACK";

    @Override
    @Transactional
    public Boolean handle(List<LimitRequest> requestList) throws TException {
        dataConsistencyCheckingService.checkLimitsExistance(requestList, LOG_PREFIX);

        List<String> operationsIds = requestList.stream()
                .map(request -> request.getOperationId())
                .toList();
        int updatedRowsCount = operationDao.rollback(operationsIds);
        if (updatedRowsCount != operationsIds.size()) {
            log.warn("[{}] Count of updated rows ({}) is not equal to the count of source rollback operations ({})",
                    LOG_PREFIX, updatedRowsCount, operationsIds.size());
        }
        return true;
    }
}
