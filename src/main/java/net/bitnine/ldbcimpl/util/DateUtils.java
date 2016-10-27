package net.bitnine.ldbcimpl.util;

import java.util.Date;

/**
 * Created by ktlee on 16. 10. 12.
 */
public class DateUtils {
    public static Date endDate(Date startDate, int days) {
        return new Date(startDate.getTime() + (long)days * 24L * 60L * 60L * 1000L);
    }
}
