package com.empayre.liminator.dao;

import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;

import java.util.List;

public interface OperationStateHistoryDao extends CommonDao<OperationStateHistory> {

    void saveBatch(List<OperationStateHistory> historyList);
}
