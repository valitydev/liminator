package com.empayre.liminator.handler;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.service.LimitsGettingService;
import dev.vality.liminator.LimitRequest;
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
public class CommitLimitAmountHandler implements Handler<List<LimitRequest>, Boolean> {

    private final OperationDao operationDao;
    private final LimitsGettingService limitsGettingService;

    private static final String LOG_PREFIX = "COMMIT";

    @Override
    @Transactional
    public Boolean handle(List<LimitRequest> requestList) throws TException {
        if (CollectionUtils.isEmpty(requestList)) {
            log.warn("[{}] Received LimitRequest list is empty", LOG_PREFIX);
            return true;
        }
        limitsGettingService.get(requestList, LOG_PREFIX);

        List<String> operationsIds = requestList.stream()
                .map(request -> request.getOperationId())
                .toList();
        int updatedRowsCount = operationDao.commit(operationsIds);
        if (updatedRowsCount != operationsIds.size()) {
            log.warn("[{}] Count of updated rows ({}) is not equal to the count of source commit operations ({})",
                    LOG_PREFIX, updatedRowsCount, operationsIds.size());
        }
        return true;
    }
}
