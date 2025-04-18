package dev.vality.liminator.dao.impl;

import dev.vality.liminator.dao.LimitContextDao;
import dev.vality.liminator.domain.tables.pojos.LimitContext;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;

import static dev.vality.liminator.domain.Tables.LIMIT_CONTEXT;

@Component
@RequiredArgsConstructor
public class LimitContextDaoImpl implements LimitContextDao {

    private final DSLContext dslContext;

    @Override
    public Long save(LimitContext limitContext) {
        return dslContext
                .insertInto(LIMIT_CONTEXT)
                .set(dslContext.newRecord(LIMIT_CONTEXT, limitContext))
                .returning(LIMIT_CONTEXT.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public LimitContext getLimitContext(Long limitId) {
        return dslContext
                .selectFrom(LIMIT_CONTEXT)
                .where(LIMIT_CONTEXT.LIMIT_DATA_ID.eq(limitId))
                .fetchOneInto(LimitContext.class);
    }
}
