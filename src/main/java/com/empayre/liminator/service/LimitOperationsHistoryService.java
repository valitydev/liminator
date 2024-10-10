package com.empayre.liminator.service;

import com.empayre.liminator.converter.OperationStateHistoryConverter;
import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.LimitValue;
import dev.vality.liminator.LimitRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;

@Service
@RequiredArgsConstructor
public class LimitOperationsHistoryService {

    private final OperationStateHistoryDao operationStateHistoryDao;
    private final OperationStateHistoryConverter operationStateHistoryConverter;

    public int[] writeOperations(LimitRequest request, OperationState state) {
        return operationStateHistoryDao.saveBatch(operationStateHistoryConverter.convert(request, state));
    }

    public List<LimitValue> getCurrentLimitValue(List<String> limitNames) {
        return operationStateHistoryDao.getCurrentLimitValue(limitNames);
    }

    public List<LimitValue> getCurrentLimitValue(List<String> limitNames, String operationId) {
        return operationStateHistoryDao.getCurrentLimitValue(limitNames, operationId);
    }

    public List<OperationStateHistory> get(String operationId,
                                           Collection<String> limitNames,
                                           List<OperationState> states) {
        return operationStateHistoryDao.get(operationId, limitNames, states);
    }
}
