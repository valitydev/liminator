package com.empayre.liminator.dao;

import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.Operation;

import java.util.Collection;
import java.util.List;

public interface OperationDao extends CommonDao<Operation> {

    void saveBatch(List<Operation> operations);

    Operation get(Long id);

    List<Operation> get(String operationId, List<OperationState> states);

    List<Operation> get(String operationId, Collection<Long> limitIds, List<OperationState> states);

    int commit(String operationId, List<Long> limitIds);

    int rollback(String operationId, List<Long> limitIds);
}
