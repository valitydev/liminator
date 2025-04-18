package dev.vality.liminator.dao.impl;

import dev.vality.liminator.dao.OperationStateHistoryDao;
import dev.vality.liminator.domain.enums.OperationState;
import dev.vality.liminator.domain.tables.pojos.OperationStateHistory;
import dev.vality.liminator.model.CurrentLimitValue;
import dev.vality.liminator.model.LimitValue;
import lombok.RequiredArgsConstructor;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

import static dev.vality.liminator.domain.Tables.LIMIT_DATA;
import static dev.vality.liminator.domain.Tables.OPERATION_STATE_HISTORY;
import static org.jooq.impl.DSL.select;

@Component
@RequiredArgsConstructor
public class OperationStateHistoryDaoImpl implements OperationStateHistoryDao {

    private final DSLContext dslContext;
    private final RecordMapper<Record, LimitValue> recordMapper;

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
    public List<LimitValue> getLimitHistory(List<String> limitNames) {
        return dslContext
                .select()
                .from(OPERATION_STATE_HISTORY.join(LIMIT_DATA)
                        .on(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.eq(LIMIT_DATA.ID))
                        .and(LIMIT_DATA.NAME.in(limitNames)))
                .fetch()
                .map(recordMapper);
    }

    @Override
    public List<LimitValue> getLimitHistory(List<String> limitNames, String operationId) {
        return dslContext
                .select()
                .from(OPERATION_STATE_HISTORY.join(LIMIT_DATA)
                        .on(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.eq(LIMIT_DATA.ID))
                        .and(LIMIT_DATA.NAME.in(limitNames)))
                .where(OPERATION_STATE_HISTORY.CREATED_AT.le(
                        select(OPERATION_STATE_HISTORY.CREATED_AT)
                                .from(OPERATION_STATE_HISTORY)
                                .where(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                                .limit(1)
                        )
                )
                .fetch()
                .map(recordMapper);
    }

    @Override
    public List<OperationStateHistory> get(String operationId,
                                           Collection<Long> limitIds,
                                           List<OperationState> states) {
        return dslContext
                .select()
                .from(OPERATION_STATE_HISTORY)
                .where(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                .and(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.in(limitIds))
                .and(OPERATION_STATE_HISTORY.STATE.in(states))
                .fetchInto(OperationStateHistory.class);
    }

    @Override
    public List<CurrentLimitValue> getCurrentValues(List<String> limitNames) {
        return getCurrentValues(limitNames, null);
    }

    @Override
    public List<CurrentLimitValue> getCurrentValues(List<String> limitNames, String operationId) {
        var holdOps = OPERATION_STATE_HISTORY.as("hold_ops");
        var commitOps = OPERATION_STATE_HISTORY.as("commit_ops");
        var rollbackOps = OPERATION_STATE_HISTORY.as("rollback_ops");
        var createdAtSelect = select(OPERATION_STATE_HISTORY.CREATED_AT)
                .from(OPERATION_STATE_HISTORY)
                .where(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                .orderBy(OPERATION_STATE_HISTORY.CREATED_AT.desc())
                .limit(1);
        var zero = DSL.zero().cast(Long.class);
        var query = dslContext
                .select(
                        LIMIT_DATA.LIMIT_ID,
                        LIMIT_DATA.NAME,
                        DSL.sum(DSL.when(
                                DSL.coalesce(commitOps.OPERATION_VALUE, 0).notEqual(0)
                                        .or(DSL.coalesce(rollbackOps.OPERATION_VALUE, 0).notEqual(0)), zero)
                                        .otherwise(DSL.coalesce(holdOps.OPERATION_VALUE, 0).cast(Long.class))
                                ).cast(Long.class),
                        DSL.sum(DSL.coalesce(commitOps.OPERATION_VALUE, 0).cast(Long.class))
                )
                .from(
                        LIMIT_DATA
                                .leftJoin(holdOps)
                                .on(
                                        LIMIT_DATA.ID.eq(holdOps.LIMIT_DATA_ID)
                                                .and(holdOps.STATE.eq(OperationState.HOLD))
                                                .and(operationId == null
                                                        ? DSL.trueCondition()
                                                        : holdOps.CREATED_AT.le(createdAtSelect))
                                )
                                .leftJoin(commitOps)
                                .on(
                                        commitOps.OPERATION_ID.eq(holdOps.OPERATION_ID)
                                                .and(commitOps.LIMIT_DATA_ID.eq(holdOps.LIMIT_DATA_ID))
                                                .and(commitOps.STATE.in(OperationState.COMMIT))
                                                .and(operationId == null
                                                        ? DSL.trueCondition()
                                                        : commitOps.CREATED_AT.le(createdAtSelect))
                                )
                                .leftJoin(rollbackOps)
                                .on(
                                        rollbackOps.OPERATION_ID.eq(holdOps.OPERATION_ID)
                                                .and(rollbackOps.LIMIT_DATA_ID.eq(holdOps.LIMIT_DATA_ID))
                                                .and(rollbackOps.STATE.in(OperationState.ROLLBACK))
                                                .and(operationId == null
                                                        ? DSL.trueCondition()
                                                        : rollbackOps.CREATED_AT.le(createdAtSelect))
                                )
                )
                .where(LIMIT_DATA.NAME.in(limitNames))
                .groupBy(LIMIT_DATA.LIMIT_ID, LIMIT_DATA.NAME);
        return query
                .fetch()
                .stream()
                .map(record ->
                        new CurrentLimitValue(
                                record.value1(),
                                record.value2(),
                                record.value3().longValue(),
                                record.value4().longValue()
                        )
                )
                .toList();
    }

    @Override
    public List<LimitValue> getHoldLimitValues(Collection<String> limitNames, String operationId) {
        return dslContext
                .select()
                .from(OPERATION_STATE_HISTORY
                        .join(LIMIT_DATA)
                        .on(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.eq(LIMIT_DATA.ID))
                        .and(OPERATION_STATE_HISTORY.OPERATION_ID.eq(operationId))
                        .and(OPERATION_STATE_HISTORY.STATE.eq(OperationState.HOLD))
                        .and(LIMIT_DATA.NAME.in(limitNames)))
                .fetch()
                .map(recordMapper);
    }
}
