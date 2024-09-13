package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.model.LimitValue;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.empayre.liminator.domain.Tables.LIMIT_DATA;
import static com.empayre.liminator.domain.Tables.OPERATION;
import static org.jooq.impl.DSL.raw;
import static org.jooq.impl.DSL.val;

@Component
@RequiredArgsConstructor
public class OperationDaoImpl implements OperationDao {

    private final DSLContext dslContext;

    private static final String DELIMITER = " ,";

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
    public int commit(List<String> limitNames, String operationId) {
        return updateStateForHoldOperation(limitNames, operationId, OperationState.COMMIT);
    }

    @Override
    public int rollback(List<String> limitNames, String operationId) {
        return updateStateForHoldOperation(limitNames, operationId, OperationState.ROLLBACK);
    }

    private int updateStateForHoldOperation(List<String> limitNames, String operationId, OperationState state) {
        return dslContext
                .update(OPERATION)
                .set(OPERATION.STATE, state)
                .where(OPERATION.OPERATION_ID.eq(operationId))
                .and(OPERATION.LIMIT_ID.in(
                        dslContext
                                .select(LIMIT_DATA.ID)
                                .from(LIMIT_DATA)
                                .where(LIMIT_DATA.NAME.in(limitNames))
                ))
                .and(OPERATION.STATE.eq(OperationState.HOLD))
                .execute();
    }

    @Override
    public List<LimitValue> getCurrentLimitValue(List<String> limitNames) {
        String sql = """
                with hold_data as (
                    select ld.id, ld.name, coalesce(sum(ops.operation_value), 0) as hold_value
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id and ops.state = 'HOLD'
                    where ld.name in ({0})
                    group by ld.id, ld.name
                ), commit_data as (
                    select ld.id, ld.name, coalesce(sum(ops.operation_value), 0) as commit_value
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id and ops.state = 'COMMIT'
                    where ld.name in ({0})
                    group by ld.id, ld.name
                )
                                
                select cd.name as limit_name, cd.commit_value, hd.hold_value
                from commit_data as cd
                join hold_data as hd on cd.id = hd.id;
                """;
        return dslContext
                .resultQuery(sql, raw(arrayToString(limitNames)))
                .fetchInto(LimitValue.class);
    }

    @Override
    public List<LimitValue> getCurrentLimitValue(List<String> limitNames, String operationId) {
        String sql = """
                with operation_timestamp as (
                    select created_at
                    from lim.operation
                    where operation_id = {0}
                ), hold_data as (
                    select ld.id, ld.name, coalesce(sum(ops.operation_value), 0) as hold_value
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'HOLD'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                ), commit_data as (
                    select ld.id, ld.name, coalesce(sum(ops.operation_value), 0) as commit_value
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'COMMIT'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                )
                                
                select cd.name as limit_name, cd.commit_value, hd.hold_value
                from commit_data as cd
                join hold_data as hd on cd.id = hd.id;
                """;
        return dslContext
                .resultQuery(sql, val(operationId), raw(arrayToString(limitNames)))
                .fetchInto(LimitValue.class);
    }

    private static String arrayToString(List<String> strings) {
        return strings.stream()
                .map(limit -> "'%s'".formatted(limit))
                .collect(Collectors.joining(DELIMITER));
    }
}
