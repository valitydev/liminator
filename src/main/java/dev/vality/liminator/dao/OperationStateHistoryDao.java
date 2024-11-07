package dev.vality.liminator.dao;

import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.model.CurrentLimitValue;
import dev.vality.liminator.model.LimitValue;

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
