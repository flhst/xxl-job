package com.xxl.job.admin.core.corn;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

import com.xxl.job.admin.core.cron.CronExpression;

/**
 * @author hst
 * @create 2024-12-20 9:14
 * @Description:
 */
public class CornExpressionTest {

    public CornExpressionTest() throws ParseException {
    }


    @Test
    public void getNextInvalidTimeAfterTest() throws Exception {
        // 每周日运行
        CronExpression cornExpression = new CronExpression("* * * ? * 1");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parse = simpleDateFormat.parse("2023-10-01 12:00:00");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒", Locale.CHINA);
        Date nextInvalidTimeAfter = cornExpression.getNextInvalidTimeAfter(parse);
        System.out.println(sdf.format(nextInvalidTimeAfter));
        System.out.println(new SimpleDateFormat("EEEE", Locale.CHINESE).format(nextInvalidTimeAfter));
    }

    @Test
    public void getTimeAfterTest() throws Exception {
        CronExpression cornExpression = new CronExpression("0 0 0 L-10W * ?");
        String start = "2024-12-20 14:33:44";
        SimpleDateFormat sdfp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdff = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒", Locale.CHINA);
        Date parse = sdfp.parse("2024-12-20 14:33:44");
        for (int i = 0; i < 10; i++) {
            Date nextInvalidTimeAfter = cornExpression.getTimeAfter(parse);
            System.out.println(sdff.format(nextInvalidTimeAfter));
            parse = nextInvalidTimeAfter;
        }
    }


}
