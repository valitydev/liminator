package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.empayre.liminator.domain.Tables.OPERATION_STATE_HISTORY;

@Component
@RequiredArgsConstructor
public class OperationStateHistoryDaoImpl implements OperationStateHistoryDao {

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
    public void saveBatch(List<OperationStateHistory> historyList) {
        var records = historyList.stream()
                .map(history -> dslContext.newRecord(OPERATION_STATE_HISTORY, history))
                .toList();
        dslContext
                .batchInsert(records)
                .execute();
    }
}
