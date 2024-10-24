package com.empayre.liminator.dao;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.CurrentLimitValue;
import com.empayre.liminator.model.LimitValue;

import java.util.Collection;
import java.util.List;

public interface OperationStateHistoryDao extends CommonDao<OperationStateHistory> {

    int[] saveBatch(List<OperationStateHistory> historyList);

    List<LimitValue> getLimitHistory(List<String> limitNames);

    List<LimitValue> getLimitHistory(List<String> limitNames, String operationId);

    List<OperationStateHistory> get(String operationId, Collection<Long> limitIds, List<OperationState> states);

    List<CurrentLimitValue> getCurrentValues(List<String> limitNames);

    List<CurrentLimitValue> getCurrentValues(List<String> limitNames, String operationId);
}
