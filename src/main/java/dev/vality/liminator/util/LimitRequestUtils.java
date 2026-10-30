package dev.vality.liminator.util;

import dev.vality.liminator.LimitChange;
import dev.vality.liminator.LimitRequest;
import lombok.experimental.UtilityClass;

@UtilityClass
public class LimitRequestUtils {

    public static boolean hasDuplicateLimitNames(LimitRequest request) {
        return request.getLimitChanges().stream()
                .map(LimitChange::getLimitName)
                .distinct()
                .count() != request.getLimitChangesSize();
    }
}
