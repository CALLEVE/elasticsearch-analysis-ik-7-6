package org.wltea.analyzer.dic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.PathUtils;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @program: elasticsearch-analysis-ik-7-6
 * @author: wangjinpeng
 * @create: 2024-01-02 21:41
 * @description:
 **/
public class CustomDictLoader {

    private static final Logger logger = ESPluginLoggerFactory.getLogger(CustomDictLoader.class.getName());

    private static final CustomDictLoader INSTANCE = new CustomDictLoader();

    private final String url;

    private final String username;

    private final String password;

//   原子类

    private final AtomicBoolean extWordFirst = new AtomicBoolean(false);

    private final AtomicBoolean stopWordFirst = new AtomicBoolean(false);

    private final AtomicReference<String> extWordLastTimeRef = new AtomicReference<>(null);

    private final AtomicReference<String> stopWordLastTimeRef = new AtomicReference<>(null);

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String WORD = "word";

    private CustomDictLoader() {
        Properties mysqlConfig = new Properties();
        Path path = PathUtils.get(Dictionary.getSingleton().getDictRoot(), "jdbc.properties");
        try {
            mysqlConfig.load(new FileInputStream(path.toFile()));
            this.url = mysqlConfig.getProperty("jdbc.url");
            this.username = mysqlConfig.getProperty("jdbc.username");
            this.password = mysqlConfig.getProperty("jdbc.password");
            logger.error("url:{}, username:{}, password:{}", url, username, password);
        } catch (Exception e) {
            throw new IllegalStateException("加载mysql数据配置错误");
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            throw new IllegalStateException("加载mysql驱动异常");
        }

    }


    public static CustomDictLoader getInstance() {
        return INSTANCE;
    }

    /**
     * 扩展关键字
     */
    public void loadMySqlExtWords() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        String sql;
        Date now = new Date();
        if (extWordFirst.compareAndSet(false, true)) {
            //初始全量加载
            sql = "SELECT word FROM extension_word;";
        } else {
            //增量
            sql = "SELECT word FROM extension_word WHERE update_time > ' " + extWordLastTimeRef.get() + "';";
        }

        extWordLastTimeRef.set(this.getNowDate());

        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            Set<String> extWords = new HashSet<>();
            while (resultSet.next()) {
                String word = resultSet.getString(WORD);
                if (Objects.nonNull(word)) {
                    extWords.add(word);
                    logger.info("从mysql加载extensionWord, word={}", word);
                }
            }
//         加入字典
            if (extWords != null) {
                Dictionary.getSingleton().addWords(extWords);
            }

        } catch (Exception e) {
            logger.error("从mysql中加载extWord失败：{}", e.getMessage());
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }

        }

    }

    private String getNowDate() {
        return SIMPLE_DATE_FORMAT.format(new Date());
    }

    /**
     * 停助词
     */
    public void loadMySqlStopWords() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        String sql;
        Date now = new Date();
        if (stopWordFirst.compareAndSet(false, true)) {
            //初始全量加载
            sql = "SELECT word FROM extension_word;";
        } else {
            //增量
            sql = "SELECT word FROM stop_word WHERE update_time > ' " + stopWordLastTimeRef.get() + "';";
        }

        stopWordLastTimeRef.set(this.getNowDate());

        try {
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            Set<String> stopWords = new HashSet<>();
            while (resultSet.next()) {
                String word = resultSet.getString(WORD);
                if (Objects.nonNull(word)) {
                    stopWords.add(word);
                    logger.info("从mysql加载stopWord, word={}", word);
                }
            }
//         加入字典
            if (stopWords != null) {
                Dictionary.getSingleton().addStopWords(stopWords);
            }

        } catch (Exception e) {
            logger.error("从mysql中加载stopWord失败：{}", e.getMessage());
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }

        }


    }

}
