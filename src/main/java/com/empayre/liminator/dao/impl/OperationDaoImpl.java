package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.AbstractDao;
import com.empayre.liminator.dao.OperationDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.Operation;
import com.empayre.liminator.domain.tables.records.OperationRecord;
import com.empayre.liminator.exception.DaoException;
import com.empayre.liminator.model.LimitValue;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

import static com.empayre.liminator.domain.Tables.OPERATION;
import static org.jooq.impl.DSL.raw;
import static org.jooq.impl.DSL.val;

@Component
public class OperationDaoImpl extends AbstractDao implements OperationDao {

    public OperationDaoImpl(DataSource dataSource) {
        super(dataSource);
    }

    private static final String DELIMITER = " ,";

    @Override
    public Long save(Operation operation) throws DaoException {
        return getDslContext()
                .insertInto(OPERATION)
                .set(getDslContext().newRecord(OPERATION, operation))
                .returning(OPERATION.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public void saveBatch(List<Operation> operations) {
        List<OperationRecord> records = operations.stream()
                .map(operation -> getDslContext().newRecord(OPERATION, operation))
                .toList();
        getDslContext().batchInsert(records).execute();
    }

    @Override
    public int commit(List<String> operationIds) {
        return updateStateForHoldOperation(operationIds, OperationState.COMMIT);
    }

    @Override
    public int rollback(List<String> operationIds) {
        return updateStateForHoldOperation(operationIds, OperationState.ROLLBACK);
    }

    private int updateStateForHoldOperation(List<String> operationIds, OperationState state) {
        return getDslContext()
                .update(OPERATION)
                .set(OPERATION.STATE, state)
                .where(OPERATION.OPERATION_ID.in(operationIds))
                .and(OPERATION.STATE.eq(OperationState.HOLD))
                .execute();
    }

    @Override
    public List<LimitValue> getCurrentLimitValue(List<String> limitNames) {
        String sql = """
                with hold_data as (
                    select ld.id, ld.name, coalesce(sum(ops.amount), 0) as hold_value
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id and ops.state = 'HOLD'
                    where ld.name in ({0})
                    group by ld.id, ld.name
                ), commit_data as (
                    select ld.id, ld.name, coalesce(sum(ops.amount), 0) as commit_value
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
        return getDslContext()
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
                    select ld.id, ld.name, coalesce(sum(ops.amount), 0) as hold_amount
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'HOLD'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                ), commit_data as (
                    select ld.id, ld.name, coalesce(sum(ops.amount), 0) as commit_amount
                    from lim.limit_data as ld
                    left join lim.operation as ops
                      on ops.limit_id = ld.id
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'COMMIT'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                )
                                
                select cd.name as limit_name, cd.commit_amount, hd.hold_amount
                from commit_data as cd
                join hold_data as hd on cd.id = hd.id;
                """;
        return getDslContext()
                .resultQuery(sql, val(operationId), raw(arrayToString(limitNames)))
                .fetchInto(LimitValue.class);
    }

    private static String arrayToString(List<String> strings) {
        return strings.stream()
                .map(limit -> "'%s'".formatted(limit))
                .collect(Collectors.joining(DELIMITER));
    }
}
