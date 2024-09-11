package com.empayre.liminator.dao;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.model.LimitValue;

import java.util.Collection;
import java.util.List;

public interface OperationDao extends CommonDao<Operation> {

    void saveBatch(List<Operation> operations);

    Operation get(Long id);

    List<Operation> get(String operationId, List<OperationState> states);

    List<Operation> get(String operationId, Collection<Long> limitIds, List<OperationState> states);

    List<LimitValue> getCurrentLimitValue(List<String> limitNames);

    List<LimitValue> getCurrentLimitValue(List<String> limitNames, String operationId);

    int commit(List<String> limitNames, String operationId);

    int rollback(List<String> limitNames, String operationId);
}
