package com.empayre.liminator.dao.impl;

import com.empayre.liminator.dao.AbstractDao;
import com.empayre.liminator.dao.LimitContextDao;
import com.empayre.liminator.domain.tables.pojos.LimitContext;
import com.empayre.liminator.exception.DaoException;
import org.jooq.impl.DataSourceConnectionProvider;
import org.springframework.stereotype.Component;

import static com.empayre.liminator.domain.Tables.LIMIT_CONTEXT;

@Component
public class LimitContextDaoImpl extends AbstractDao implements LimitContextDao {

    public LimitContextDaoImpl(DataSourceConnectionProvider dataSource) {
        super(dataSource);
    }

    @Override
    public Long save(LimitContext limitContext) throws DaoException {
        return getDslContext()
                .insertInto(LIMIT_CONTEXT)
                .set(getDslContext().newRecord(LIMIT_CONTEXT, limitContext))
                .returning(LIMIT_CONTEXT.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public LimitContext getLimitContext(Long limitId) {
        return getDslContext()
                .selectFrom(LIMIT_CONTEXT)
                .where(LIMIT_CONTEXT.LIMIT_ID.eq(limitId))
                .fetchOneInto(LimitContext.class);
    }
}
