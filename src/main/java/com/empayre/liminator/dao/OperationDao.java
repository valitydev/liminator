package com.empayre.liminator.dao;

import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.model.LimitValue;

import java.util.List;

public interface OperationDao extends CommonDao<Operation> {

    void saveBatch(List<Operation> operations);

    Operation get(Long id);

    List<LimitValue> getCurrentLimitValue(List<String> limitNames);

    List<LimitValue> getCurrentLimitValue(List<String> limitNames, String operationId);

    int commit(List<String> limitNames, String operationId);

    int rollback(List<String> limitNames, String operationId);
}
