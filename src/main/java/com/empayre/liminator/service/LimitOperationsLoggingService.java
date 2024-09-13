package com.empayre.liminator.service;

import com.empayre.liminator.converter.OperationStateHistoryConverter;
import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.domain.enums.OperationState;
import dev.vality.liminator.LimitRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LimitOperationsLoggingService {

    private final OperationStateHistoryDao operationStateHistoryDao;
    private final OperationStateHistoryConverter operationStateHistoryConverter;

    @Value("${service.logging.enabled}")
    private boolean loggingEnabled;

    public void writeOperations(LimitRequest request, OperationState state) {
        if (loggingEnabled) {
            operationStateHistoryDao.saveBatch(operationStateHistoryConverter.convert(request, state));
        }
    }
}
