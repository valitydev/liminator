package dev.vality.liminator.dao.impl;

import dev.vality.liminator.dao.OperationDao;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.Operation;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

import static dev.vality.liminator.domain.Tables.OPERATION;

@Component
@RequiredArgsConstructor
public class OperationDaoImpl implements OperationDao {

    private final DSLContext dslContext;

    @Override
    public Long save(Operation operation) {
        return dslContext
                .insertInto(OPERATION)
                .set(dslContext.newRecord(OPERATION, operation))
                .returning(OPERATION.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public Operation get(Long id) {
        return dslContext
                .selectFrom(OPERATION)
                .where(OPERATION.ID.eq(id))
                .fetchOneInto(Operation.class);
    }

    @Override
    public List<Operation> get(String operationId, List<OperationState> states) {
        return dslContext
                .selectFrom(OPERATION)
                .where(OPERATION.OPERATION_ID.eq(operationId))
                .and(OPERATION.STATE.in(states))
                .fetchInto(Operation.class);
    }

    @Override
    public List<Operation> get(String operationId, Collection<Long> limitIds, List<OperationState> states) {
        return dslContext
                .selectFrom(OPERATION)
                .where(OPERATION.OPERATION_ID.eq(operationId))
                .and(OPERATION.LIMIT_ID.in(limitIds))
                .and(OPERATION.STATE.in(states))
                .fetchInto(Operation.class);
    }

    @Override
    public void saveBatch(List<Operation> operations) {
        var inserts = operations.stream()
                .map(operation ->
                        dslContext
                                .insertInto(OPERATION)
                                .set(dslContext.newRecord(OPERATION, operation))
                                .onConflict(OPERATION.LIMIT_ID, OPERATION.OPERATION_ID)
                                .doNothing()
                )
                .toArray(Query[]::new);
        dslContext
                .batch(inserts)
                .execute();
    }

    @Override
    public int commit(String operationId, List<Long> limitIds) {
        return updateStateForHoldOperation(operationId, OperationState.COMMIT, limitIds);
    }

    @Override
    public int rollback(String operationId, List<Long> limitIds) {
        return updateStateForHoldOperation(operationId, OperationState.ROLLBACK, limitIds);
    }

    private int updateStateForHoldOperation(String operationId, OperationState state, List<Long> limitIds) {
        return dslContext
                .update(OPERATION)
                .set(OPERATION.STATE, state)
                .where(OPERATION.OPERATION_ID.eq(operationId))
                .and(OPERATION.LIMIT_ID.in(limitIds))
                .and(OPERATION.STATE.eq(OperationState.HOLD))
                .execute();
    }
}
