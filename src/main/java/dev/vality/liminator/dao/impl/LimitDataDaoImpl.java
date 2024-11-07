package dev.vality.liminator.dao.impl;

import dev.vality.liminator.dao.LimitDataDao;
import dev.vality.liminator.domain.tables.pojos.LimitData;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import static dev.vality.liminator.domain.Tables.LIMIT_DATA;

@Component
@RequiredArgsConstructor
public class LimitDataDaoImpl implements LimitDataDao {

    private final DSLContext dslContext;

    @Override
    public Long save(LimitData limitData) {
        return dslContext
                .insertInto(LIMIT_DATA)
                .set(dslContext.newRecord(LIMIT_DATA, limitData))
                .onConflict(LIMIT_DATA.NAME)
                .doUpdate()
                .set(LIMIT_DATA.WTIME, LocalDateTime.now())
                .returning(LIMIT_DATA.ID)
                .fetchOne()
                .getId();
    }

    @Override
    public LimitData get(String limitName) {
        return dslContext
                .selectFrom(LIMIT_DATA)
                .where(LIMIT_DATA.NAME.equal(limitName))
                .fetchOneInto(LimitData.class);
    }

    @Override
    public List<LimitData> get(Collection<String> limitNames) {
        return dslContext
                .selectFrom(LIMIT_DATA)
                .where(LIMIT_DATA.NAME.in(limitNames))
                .fetchInto(LimitData.class);
    }
}
