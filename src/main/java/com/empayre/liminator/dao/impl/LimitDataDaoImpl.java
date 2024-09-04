package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.AbstractDao;
import com.empayre.liminator.dao.LimitDataDao;
import com.empayre.liminator.domain.tables.pojos.LimitData;
import com.empayre.liminator.exception.DaoException;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import static com.empayre.liminator.domain.Tables.LIMIT_DATA;

@Component
public class LimitDataDaoImpl extends AbstractDao implements LimitDataDao {

    public LimitDataDaoImpl(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Long save(LimitData limitData) throws DaoException {
        return getDslContext()
                .insertInto(LIMIT_DATA)
                .set(getDslContext().newRecord(LIMIT_DATA, limitData))
                .onConflict(LIMIT_DATA.NAME)
                .doUpdate()
                .set(LIMIT_DATA.WTIME, LocalDateTime.now())
                .returning(LIMIT_DATA.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public LimitData get(String limitName) {
        return getDslContext()
                .selectFrom(LIMIT_DATA)
                .where(LIMIT_DATA.NAME.equal(limitName))
                .fetchOneInto(LimitData.class);
    }

    @Override
    public List<LimitData> get(Collection<String> limitNames) {
        return getDslContext()
                .selectFrom(LIMIT_DATA)
                .where(LIMIT_DATA.NAME.in(limitNames))
                .fetchInto(LimitData.class);
    }
}
