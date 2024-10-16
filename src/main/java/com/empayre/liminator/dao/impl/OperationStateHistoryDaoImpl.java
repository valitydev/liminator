package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.domain.enums.OperationState;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.model.LimitValue;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

import static com.empayre.liminator.domain.Tables.LIMIT_DATA;
import static com.empayre.liminator.domain.Tables.OPERATION_STATE_HISTORY;
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
                        .on(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.eq(LIMIT_DATA.ID)))
                .where(OPERATION_STATE_HISTORY.LIMIT_NAME.in(limitNames))
                .fetch()
                .map(recordMapper);
    }

    @Override
    public List<LimitValue> getLimitHistory(List<String> limitNames, String operationId) {
        return dslContext
                .select()
                .from(OPERATION_STATE_HISTORY.join(LIMIT_DATA)
                        .on(OPERATION_STATE_HISTORY.LIMIT_DATA_ID.eq(LIMIT_DATA.ID)))
                .where(OPERATION_STATE_HISTORY.LIMIT_NAME.in(limitNames))
                .and(OPERATION_STATE_HISTORY.CREATED_AT.le(
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
}
