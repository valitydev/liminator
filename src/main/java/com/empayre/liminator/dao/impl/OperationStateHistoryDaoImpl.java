package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.LimitValue;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.empayre.liminator.domain.Tables.OPERATION_STATE_HISTORY;
import static org.jooq.impl.DSL.raw;
import static org.jooq.impl.DSL.val;

@Component
@RequiredArgsConstructor
public class OperationStateHistoryDaoImpl implements OperationStateHistoryDao {

    private static final String DELIMITER = " ,";

    private final DSLContext dslContext;

    @Override
    public Long save(OperationStateHistory history) {
        return dslContext
                .insertInto(OPERATION_STATE_HISTORY)
                .set(dslContext.newRecord(OPERATION_STATE_HISTORY, history))
                .returning(OPERATION_STATE_HISTORY.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public int[] saveBatch(List<OperationStateHistory> historyList) {
        var records = historyList.stream()
                .map(history -> dslContext.newRecord(OPERATION_STATE_HISTORY, history))
                .toList();
        return dslContext
                .batchInsert(records)
                .execute();
    }

    @Override
    public List<LimitValue> getCurrentLimitValue(List<String> limitNames) {
        String sql = """
                with hold_data as (
                    select ld.id, ld.name, ld.limit_id, coalesce(sum(ops.operation_value), 0) as hold_value
                    from lim.limit_data as ld
                    left join lim.operation_state_history as ops
                      on ops.limit_name = ld.name
                      and ops.state = 'HOLD'
                    where ld.name in ({0})
                    group by ld.id, ld.name
                ), commit_data as (
                    select ld.id, ld.name, ld.limit_id, coalesce(sum(ops.operation_value), 0) as commit_value
                    from lim.limit_data as ld
                    left join lim.operation_state_history as ops
                      on ops.limit_name = ld.name
                      and ops.state = 'COMMIT'
                    where ld.name in ({0})
                    group by ld.id, ld.name
                ), rollback_data as (
                    select ld.id, ld.name, ld.limit_id, coalesce(sum(ops.operation_value), 0) as rollback_value
                    from lim.limit_data as ld
                    left join lim.operation_state_history as ops
                      on ops.limit_name = ld.name
                      and ops.state = 'ROLLBACK'
                    where ld.name in ({0})
                    group by ld.id, ld.name
                )
                                
                select cd.limit_id, cd.name as limit_name, cd.commit_value, hd.hold_value, rd.rollback_value
                from commit_data as cd
                join hold_data as hd on cd.id = hd.id
                join rollback_data as rd on cd.id = rd.id;
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
                    from lim.operation_state_history
                    where operation_id = {0}
                ), hold_data as (
                    select ld.id, ld.name, ld.limit_id, coalesce(sum(ops.operation_value), 0) as hold_value
                    from lim.limit_data as ld
                    left join lim.operation_state_history as ops
                      on ops.limit_name = ld.name
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'HOLD'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                ), commit_data as (
                    select ld.id, ld.name, ld.limit_id, coalesce(sum(ops.operation_value), 0) as commit_value
                    from lim.limit_data as ld
                    left join lim.operation_state_history as ops
                      on ops.limit_name = ld.name
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'COMMIT'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                ), rollback_data as (
                    select ld.id, ld.name, ld.limit_id, coalesce(sum(ops.operation_value), 0) as rollback_value
                    from lim.limit_data as ld
                    left join lim.operation_state_history as ops
                      on ops.limit_name = ld.name
                     and ops.created_at <= (select created_at from operation_timestamp limit 1)
                     and ops.state = 'ROLLBACK'
                    where ld.name in ({1})
                    group by ld.id, ld.name
                )
                                
                select cd.limit_id, cd.name as limit_name, cd.commit_value, hd.hold_value, rd.rollback_value
                from commit_data as cd
                join hold_data as hd on cd.id = hd.id
                join rollback_data as rd on cd.id = rd.id;
                """;
        return dslContext
                .resultQuery(sql, val(operationId), raw(arrayToString(limitNames)))
                .fetchInto(LimitValue.class);
    }

    @Override
    public List<OperationStateHistory> get(String operationId,
                                           Collection<Long> limitIds,
                                           List<OperationState> states) {
        return dslContext
                .selectFrom(OPERATION_STATE_HISTORY)
                .where(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                .and(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.in(limitIds))
                .and(OPERATION_STATE_HISTORY.STATE.in(states))
                .fetchInto(OperationStateHistory.class);
    }

    private static String arrayToString(List<String> strings) {
        return strings.stream()
                .map(limit -> "'%s'".formatted(limit))
                .collect(Collectors.joining(DELIMITER));
    }
}
