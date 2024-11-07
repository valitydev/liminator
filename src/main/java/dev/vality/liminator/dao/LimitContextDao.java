package dev.vality.liminator.dao;

import dev.vality.liminator.domain.tables.pojos.LimitContext;

public interface LimitContextDao extends CommonDao<LimitContext> {

    LimitContext getLimitContext(Long limitId);
}
