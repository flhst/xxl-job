package com.xxl.job.admin.core.util;

import freemarker.ext.beans.BeansWrapper;
import freemarker.ext.beans.BeansWrapperBuilder;
import freemarker.template.Configuration;
import freemarker.template.TemplateHashModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ftl util
 *
 * @author xuxueli 2018-01-17 20:37:48
 */
public class FtlUtil {
    private static Logger logger = LoggerFactory.getLogger(FtlUtil.class);

    private static BeansWrapper wrapper = new BeansWrapperBuilder(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS).build();     //BeansWrapper.getDefaultInstance();

    // 从给定的包名中获取静态模型。具体步骤如下：
    // 获取所有静态模型：通过 wrapper.getStaticModels() 获取所有静态模型。
    // 根据包名获取特定静态模型：从所有静态模型中，通过包名获取特定的静态模型。
    // 异常处理：如果在获取过程中发生异常，记录错误日志并返回 null。
    public static TemplateHashModel generateStaticModel(String packageName) {
        try {
            TemplateHashModel staticModels = wrapper.getStaticModels();
            TemplateHashModel fileStatics = (TemplateHashModel) staticModels.get(packageName);
            return fileStatics;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

}
