package com.empayre.liminator.dao;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.LimitValue;

import java.util.Collection;
import java.util.List;

public interface OperationStateHistoryDao extends CommonDao<OperationStateHistory> {

    int[] saveBatch(List<OperationStateHistory> historyList);

    List<LimitValue> getCurrentLimitValue(List<String> limitNames);

    List<LimitValue> getCurrentLimitValue(List<String> limitNames, String operationId);

    List<OperationStateHistory> get(String operationId, Collection<String> limitNames, List<OperationState> states);
}
