package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.AbstractDao;
import com.empayre.liminator.dao.OperationStateHistoryDao;
import com.empayre.liminator.domain.tables.pojos.OperationStateHistory;
import com.empayre.liminator.exception.DaoException;
import org.jooq.impl.DataSourceConnectionProvider;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.empayre.liminator.domain.Tables.OPERATION_STATE_HISTORY;

@Component
public class OperationStateHistoryDaoImpl extends AbstractDao implements OperationStateHistoryDao {

    public OperationStateHistoryDaoImpl(DataSourceConnectionProvider dataSource) {
        super(dataSource);
    }

    @Override
    public Long save(OperationStateHistory history) throws DaoException {
        return getDslContext()
                .insertInto(OPERATION_STATE_HISTORY)
                .set(getDslContext().newRecord(OPERATION_STATE_HISTORY, history))
                .returning(OPERATION_STATE_HISTORY.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public void saveBatch(List<OperationStateHistory> historyList) {
        var records = historyList.stream()
                .map(history -> getDslContext().newRecord(OPERATION_STATE_HISTORY, history))
                .toList();
        getDslContext()
                .batchInsert(records)
                .execute();
    }
}
