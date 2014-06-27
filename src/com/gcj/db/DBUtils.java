package com.gcj.db;

import com.gcj.bean.NewsInfoBean;
import com.gcj.utils.DateUtils;
import com.gcj.utils.DoubleUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class DBUtils {

    private static final Log LOG = LogFactory.getLog(DBUtils.class);

    private Connection conn;

    private Statement stmt;

    private PreparedStatement pstmt1;

    private PreparedStatement pstmt2;

    private ResultSet rs;

    private String dbName;

    private List<Integer> sameNewsIdList = new ArrayList<Integer>();

    Set<Integer> dupNewsId = new HashSet<Integer>();

    private Set<Integer> reserveNewsId = new HashSet<Integer>();

    private Map<Integer, Integer> newsCodeCountMap = new HashMap<Integer, Integer>();

    Map<Integer, Integer> newsIdSameNewsIdMap = new HashMap<Integer, Integer>();


    public void connToSQLServer() throws ClassNotFoundException, SQLException {
        String ip = "127.0.0.1";
        int port = 1433;
        String user = "sa";
        String passwd = "111111";
        String url = "jdbc:sqlserver://" + ip + ":" + port + ";databaseName=" + dbName + ";user=" + user + ";password=" + passwd;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        conn = DriverManager.getConnection(url);
    }

    private Connection connToMySQL() {
        Connection conn = null;
        String ip = "127.0.0.1";
        int port = 3306;
        String user = "root";
        String pwd = "111111";
        String url = "jdbc:mysql://" + ip + ":" + port + "/ncps?useUnicode=true&characterEncoding=utf8";
        String driver = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    private void createStmt() throws SQLException {
        stmt = conn.createStatement();
    }

    /**
     * 查询表nw0001，查看保留的新闻的标记是为1
     */
    private void selectNw0001() {

        int errorIsCdCount = 0;
        //String sSql = "select NewsCode,NW0001_002,NW0001_003,ISCD,deletedTag from nw0001 where NewsCode = ?";

        String sSql = "select NewsCode,ISCD,NW0001_003 from nw0001";

        Map<Integer, Integer> newsCodeIscdMap = new HashMap<Integer, Integer>();

        Connection conn = connToMySQL();

        PreparedStatement pstmt = null;

        ResultSet rs = null;

        try {

            connToSQLServer();

            pstmt1 = this.conn.prepareStatement(sSql);
            this.rs = pstmt1.executeQuery();

            double total = 0;
            double errorCount = 0;

            String select_news_duprmv_content = "select ID2 from news_duprmv_content where ID=?";

            pstmt = conn.prepareStatement(select_news_duprmv_content);


            while (this.rs.next()) {
                int newsCode = this.rs.getInt(1);
                int iscd = this.rs.getInt(2);
                String nw0001_003 = this.rs.getString(3);
                total++;
                if (iscd == 0) {
                    if (nw0001_003 != null && !nw0001_003.equals("") && !nw0001_003.equals(" ")) {
                        LOG.error("出现iscd=0，但是正文不为空的现象：select NewsCode,ISCD,NW0001_003 from nw0001 where NewsCode = " + newsCode);
                        errorCount++;
                        pstmt.setInt(1, newsCode);
                        rs = pstmt.executeQuery();
                        int id2 = -1;
                        while (rs.next()) {
                            id2 = rs.getInt(1);
                        }

                        if (id2 == -1) {
                            LOG.error("NewsCode = " + newsCode + "在表news_duprmv_content中的ID2为" + id2 + "......Test Pass[ERROR]");
                        } else if (id2 == 0) {
                            LOG.info("NewsCode = " + newsCode + "在表news_duprmv_content中的ID2为" + id2 + "......Test Pass[OK]");
                        } else {
                            LOG.error("NewsCode = " + newsCode + "在表news_duprmv_content中的ID2为" + id2 + "......Test Pass[ERROR]");
                        }
                    }
                }
                newsCodeIscdMap.put(newsCode, iscd);
            }

            double percent = (errorCount / total) * 100;

            LOG.info("出现iscd=0，但是正文不为空的现象所占的比例：（" + errorCount + " / " + total + ") * 100 = " + DoubleUtils.convert(percent) + "%");

            //pstmt1 = conn.prepareStatement(sSql);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (this.rs != null)
                    this.rs.close();
                if (pstmt1 != null)
                    pstmt1.close();
                if (this.conn != null)
                    this.conn.close();
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        for (Integer sameNewsId : reserveNewsId) {
            //pstmt1.setInt(1, sameNewsId);
            //rs = pstmt1.executeQuery();
            //while (rs.next()) {
            //int iscd = rs.getInt(4);
            if (newsCodeIscdMap.containsKey(sameNewsId)) {
                int iscd = newsCodeIscdMap.get(sameNewsId);
                if (iscd != 1) {
                    LOG.error("被保留的新闻ID的标记不为1！: " + sameNewsId + "-----> ISCD: " + iscd + " SQL: select NewsCode,ISCD from nw0001 where NewsCode = " + sameNewsId + ";select NEWSID,SAMENEWSID from SAMENEWS_P where NEWSID = " + sameNewsId + ";select NEWSID,SAMENEWSID from SAMENEWS_P where SAMENEWSID = " + sameNewsId);
                    errorIsCdCount++;
                }
            } else {
                LOG.error("被保留的新闻ID: " + sameNewsId + ".根据该新闻ID在NW0001查询不到记录！");
            }

            //}
        }
        for (Integer sameNewsId : dupNewsId) {
            if (newsCodeIscdMap.containsKey(sameNewsId)) {
                int iscd = newsCodeIscdMap.get(sameNewsId);
                if (iscd != 2) {
                    LOG.error("新闻ID为：" + sameNewsId + "的新闻未被打上被去重标记！ISCD = " + iscd);
                }
            }
        }

        double percent = ((double) errorIsCdCount / (double) reserveNewsId.size()) * 100;
        LOG.debug("共发现保留的newid的ISCD未被置为1：" + errorIsCdCount + "。占：" + errorIsCdCount + " / " + reserveNewsId.size() + " = " + DoubleUtils.convert(percent) + "%.");
    }

    /**
     * 查询表samenews
     */
    private void selectSameNews() {
        String sSql = "select NEWSID,SAMENEWSID from SAMENEWS";
        LOG.debug("正在查询新闻去重结果表SAMENEWS，请等待......");
        try {
            connToSQLServer();
            createStmt();
            rs = stmt.executeQuery(sSql);
            while (rs.next()) {
                int newsId = rs.getInt(1);
                int sameNewsId = rs.getInt(2);
                LOG.debug("NewsId-->SameNewsId: " + newsId + "-->" + sameNewsId);
                if (newsIdSameNewsIdMap.containsKey(newsId)) {
                    LOG.error("重复的NewsId: " + newsId); //发现重复的newsid,说明有问题
                } else {
                    newsIdSameNewsIdMap.put(newsId, sameNewsId);
                }
                sameNewsIdList.add(sameNewsId);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        LOG.debug("表SAMENEWS_P处理结束.......[OK]");

        LOG.debug("计算是否存在被保留的新闻被更高优先级的新闻去重了......");

        //Set<Integer> dupNewsId = new HashSet<Integer>();

        for (Map.Entry<Integer, Integer> entry : newsIdSameNewsIdMap.entrySet()) {

            //int newsId = entry.getKey();
            int sameNewsId = entry.getValue();

//            if (newsCodeCountMap.containsKey(newsId)) {
//                int count = newsCodeCountMap.get(newsId);
//                count++;
//                newsCodeCountMap.put(newsId, count);
//            } else {
//                newsCodeCountMap.put(newsId, 1);
//            }
//
//            if (newsCodeCountMap.containsKey(sameNewsId)) {
//                int count = newsCodeCountMap.get(sameNewsId);
//                count++;
//                newsCodeCountMap.put(sameNewsId, count);
//            } else {
//                newsCodeCountMap.put(sameNewsId, 1);
//            }
//            count = newsIdSameNewsIdMap.get(sameNewsId);
//            count++;
//            newsCodeCountMap.put(sameNewsId, count);

            //dupNewsId.add(newsId);


            if (newsIdSameNewsIdMap.containsKey(sameNewsId)) {
                LOG.debug("SAMENEWSID: " + sameNewsId + "——在把一条新闻去重之后，被更高优先级的新闻去重了！");
                dupNewsId.add(sameNewsId);
            } else {
                reserveNewsId.add(sameNewsId);
            }
        }
    }

    private void selectSameNewsP() {

        String sSql = "select NEWSID,SAMENEWSID from SAMENEWS_P";
        LOG.debug("正在查询新闻去重结果表SAMENEWS_P，请等待......");
        try {
            connToSQLServer();
            createStmt();
            rs = stmt.executeQuery(sSql);
            newsIdSameNewsIdMap.clear();
            sameNewsIdList.clear();
            while (rs.next()) {
                int newsId = rs.getInt(1);
                int sameNewsId = rs.getInt(2);
                LOG.debug("NewsId-->SameNewsId: " + newsId + "-->" + sameNewsId);
                if (newsIdSameNewsIdMap.containsKey(newsId)) {
                    LOG.error("重复的NewsId: " + newsId); //发现重复的newsid,说明有问题
                } else {
                    newsIdSameNewsIdMap.put(newsId, sameNewsId);
                }
                sameNewsIdList.add(sameNewsId);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        LOG.debug("表SAMENEWS_P处理结束.......[OK]");

        //Set<Integer> dupNewsId = new HashSet<Integer>();

        LOG.debug("计算是否存在被保留的新闻被更高优先级的新闻去重了......");

        dupNewsId.clear();
        reserveNewsId.clear();

        for (Map.Entry<Integer, Integer> entry : newsIdSameNewsIdMap.entrySet()) {

            //int newsId = entry.getKey();
            int sameNewsId = entry.getValue();

//            if (newsCodeCountMap.containsKey(newsId)) {
//                int count = newsCodeCountMap.get(newsId);
//                count++;
//                newsCodeCountMap.put(newsId, count);
//            } else {
//                newsCodeCountMap.put(newsId, 1);
//            }
//
//            if (newsCodeCountMap.containsKey(sameNewsId)) {
//                int count = newsCodeCountMap.get(sameNewsId);
//                count++;
//                newsCodeCountMap.put(sameNewsId, count);
//            } else {
//                newsCodeCountMap.put(sameNewsId, 1);
//            }
//            count = newsIdSameNewsIdMap.get(sameNewsId);
//            count++;
//            newsCodeCountMap.put(sameNewsId, count);

            //dupNewsId.add(newsId);


            if (newsIdSameNewsIdMap.containsKey(sameNewsId)) {
                LOG.debug("SAMENEWSID: " + sameNewsId + "——在把一条新闻去重之后，被更高优先级的新闻去重了！");
                dupNewsId.add(sameNewsId);
            } else {
                reserveNewsId.add(sameNewsId);
            }
        }
    }

    public void selectNw2011() {

        String sSql = "select NewsCode,CDNum from NW2011";

        Map<Integer, Integer> newsCodeCdNumMap = new HashMap<Integer, Integer>();

        String select_SameNewsP = "select NEWSID from SAMENEWS_P where SAMENEWSID = ?";

        try {
            connToSQLServer();

            createStmt();

            rs = stmt.executeQuery(sSql);

            while (rs.next()) {

                int newsCode = rs.getInt(1);
                int cdNum = rs.getInt(2);
                newsCodeCdNumMap.put(newsCode, cdNum);

            }

            pstmt1 = conn.prepareStatement(select_SameNewsP);

            for (Integer newsId : reserveNewsId) {

                pstmt1.setInt(1, newsId);

                rs = pstmt1.executeQuery();

                int count;

                List<Integer> newsIdList = new ArrayList<Integer>();

                while (rs.next()) {
                    int newsCode = rs.getInt(1);
                    newsIdList.add(newsCode);
                }

                count = newsIdList.size();

                for (Integer newsCode : newsIdList) {
                    pstmt1.setInt(1, newsCode);
                    rs = pstmt1.executeQuery();
                    while (rs.next()) {
                        count++;
                    }
                }

                if (count == 0) {
                    LOG.error("NewsCode: " + newsId + "在表SAMENEWS中无法找到值！");
                } else {
                    if (newsCodeCdNumMap.containsKey(newsId)) {
                        int expertCount = newsCodeCdNumMap.get(newsId);
                        if (expertCount == count) {
                            LOG.debug("NewsCode: " + newsId + ".Correct!");
                        } else {
                            LOG.error("NewsCode为" + newsId + "的次数统计错误.ERROR!-select count(*) from SAMENEWS_P where SAMENEWSID = " + newsId + "; select CDNum from NW2011 where NewsCode = " + newsId);
                        }
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (stmt != null)
                    stmt.close();
                if (pstmt1 != null)
                    pstmt1.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteData(String tableName) {
        String dSql = "delete from " + tableName;
        try {
            connToSQLServer();
            createStmt();
            LOG.info("正在清空表" + tableName + "中的数据，请等待......");
            stmt.execute(dSql);
            LOG.info("表" + tableName + "中的数据已清空完成！");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 去重逻辑
     */
    private void priorityDupNews() {

        //String sSql = "select NW0001_001 from nw0001 where NewsCode = ?";
        //String sSql = "select b.slevel,a.nw0001_001 from (select * from nw0001 where newscode = ?) a left join nw0018 b on a.nw0001_004 = b.groupname and a.channel = b.sitename and a.nw0001_006 = b.channel";
        String select_nw0018 = "select slevel,groupname,sitename,channel from nw0018";
        String seletc_nw0009 = "select ISCD,GROUPNAME,SITENAME,CHANNEL from NW0009";
        String selectNw0001_001 = "select newscode,nw0001_001,nw0001_004,channel,nw0001_006,EntryDate,EntryTime from nw0001";
        Map<Integer, NewsInfoBean> newsInfoBeanMap = new HashMap<Integer, NewsInfoBean>();
        Map<String, Integer> priorityMap = new HashMap<String, Integer>();
        Map<String, Integer> iscdMap = new HashMap<String, Integer>();

        try {

            connToSQLServer();

            createStmt();

            rs = stmt.executeQuery(selectNw0001_001);

            while (rs.next()) {

                int newsCode = rs.getInt(1);
                String updateTime = rs.getString(2);
                String groupName = rs.getString(3);
                String siteName = rs.getString(4);
                String channel = rs.getString(5);
                String entryDate = rs.getString(6);
                String entryTime = rs.getString(7);

                //LOG.info("NewsCode: " + newsCode + "  UpdateTime: " + updateTime);

                NewsInfoBean newsInfoBean = new NewsInfoBean();
                newsInfoBean.setUpdateTime(updateTime);
                newsInfoBean.setGroupName(groupName);
                newsInfoBean.setSiteName(siteName);
                newsInfoBean.setChannel(channel);
                newsInfoBean.setEntryDate(entryDate);
                newsInfoBean.setEntryTime(entryTime);
                newsInfoBeanMap.put(newsCode, newsInfoBean);
            }

            rs = stmt.executeQuery(select_nw0018);

            while (rs.next()) {
                int slevel = rs.getInt(1);
                String groupName = rs.getString(2);
                String siteName = rs.getString(3);
                String channel = rs.getString(4);
                priorityMap.put(groupName + ":" + siteName + ":" + channel, slevel);
            }

            rs = stmt.executeQuery(seletc_nw0009);
            while (rs.next()) {
                int iscd = rs.getInt(1);
                String groupName = rs.getString(2);
                String siteName = rs.getString(3);
                String channel = rs.getString(4);
                iscdMap.put(groupName + ":" + siteName + ":" + channel, iscd);
            }

            //pstmt1 = conn.prepareStatement(select_nw0018);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt1 != null)
                    pstmt1.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        for (Map.Entry<Integer, Integer> entry : newsIdSameNewsIdMap.entrySet()) {

            int newsId = entry.getKey();
            int sameNewsId = entry.getValue();

            LOG.debug("NewsId: " + newsId + "    SameNewsId: " + sameNewsId);

//            NewsInfoBean newsInfoBean1 = getNewsInfo(newsId);
//            NewsInfoBean newsInfoBean2 = getNewsInfo(sameNewsId);
            if (!newsInfoBeanMap.containsKey(newsId)) {
                LOG.error("NewsId为 " + newsId + "在nw0001中不存在！");
            }

            if (!newsInfoBeanMap.containsKey(sameNewsId)) {
                LOG.error("SameNewsId为 " + newsId + "在nw0001中不存在！");
            }

            if (!newsInfoBeanMap.containsKey(newsId) || !newsInfoBeanMap.containsKey(sameNewsId))
                continue;

            NewsInfoBean newsInfoBean1 = newsInfoBeanMap.get(newsId);
            NewsInfoBean newsInfoBean2 = newsInfoBeanMap.get(sameNewsId);

            String key1 = getKey(newsInfoBean1.getGroupName(), newsInfoBean1.getSiteName(), newsInfoBean1.getChannel());
            String key2 = getKey(newsInfoBean2.getGroupName(), newsInfoBean2.getSiteName(), newsInfoBean2.getChannel());
//            int priority1 = newsInfoBean1.getSlevel();
//            int priority2 = newsInfoBean2.getSlevel();

            int iscd1 = -1;

            if (iscdMap.containsKey(key1)) {
                iscd1 = iscdMap.get(key1);
            }

            int iscd2 = -1;

            if (iscdMap.containsKey(key2)) {
                iscd2 = iscdMap.get(key2);
            }

            if (iscd1 == 0 && iscd2 != 0) {
                LOG.error("根据ISCD判断，NEWSID的ISCD为0（拥有最高权限），SAMENEWSID的ISCD却不为0" + "NewsId(ISCD: " + iscd1 + ")-->SameNewsId(ISCD: " + iscd2 + "): " + newsId + "-->" + sameNewsId + ": ERROR!");
                continue;
            } else if (iscd1 != 0 && iscd2 == 0) {

                LOG.debug("Correct");
                continue;

            } else if (iscd1 == 0) {

                String entryDate1 = newsInfoBean1.getEntryDate();
                String entryTime1 = newsInfoBean1.getEntryTime();
                String[] entryDate1S = entryDate1.split(" ");
                String updateTime1 = entryDate1S[0] + " " + entryTime1;

                String entryDate2 = newsInfoBean2.getEntryDate();
                String entryTime2 = newsInfoBean2.getEntryTime();
                String[] entryDate2S = entryDate2.split(" ");
                String updateTime2 = entryDate2S[0] + " " + entryTime2;

                //Date date1 = DateUtils.str2Date(newsInfoBean1.getUpdateTime());
                //Date date2 = DateUtils.str2Date(newsInfoBean2.getUpdateTime());
                Date date1 = DateUtils.str2Date(updateTime1);
                Date date2 = DateUtils.str2Date(updateTime2);


                if (date1.after(date2) || date1.equals(date2)) {
                    LOG.debug("逻辑去重：NewsId-->SameNewsId: " + newsId + "-->" + sameNewsId + ": OK!");
                } else {
                    //LOG.info("News Date: " + date1);
                    //LOG.info("SameNewsId Date: " + date2);
                    LOG.error("两者的ISCD均为0，根据时间判断两者是否符合去重逻辑！" + "NewsId(" + date1 + ")(ISCD: " + iscd1 + ")-->SameNewsId(" + date2 + ")(ISCD: " + iscd2 + "): " + newsId + "-->" + sameNewsId + ": ERROR!");
                }

                continue;
            }

            int priority1 = 0;

            if (priorityMap.containsKey(key1)) {
                priority1 = priorityMap.get(key1);
                //LOG.info("News Priority: " + priority1);
            } else {
                //LOG.warn("GroupName:SiteName:Channel组合找不到优先级：" + key1);
            }

            int priority2 = 0;

            if (priorityMap.containsKey(key2)) {
                priority2 = priorityMap.get(key2);
                //LOG.info("SameNewsId Priority: " + priority2);
            } else {
                //LOG.warn("GroupName:SiteName:Channel组合找不到优先级：" + key2);
            }

            if ((priority2 < priority1) || (priority1 == 0 && priority2 != 0)) {
                LOG.debug("根据优先级去重正确：NewsId-->SameNewsId: " + newsId + "-->" + sameNewsId + ": OK!");
            } else if (priority2 == priority1) {
                //Date date1 = getPubDate(newsId);
                //Date date2 = getPubDate(sameNewsId);
                //Date date1 = DateUtils.str2Date(newsInfoBean1.getUpdateTime());
                //Date date2 = DateUtils.str2Date(newsInfoBean2.getUpdateTime());

                String entryDate1 = newsInfoBean1.getEntryDate();
                String entryTime1 = newsInfoBean1.getEntryTime();
                String[] entryDate1S = entryDate1.split(" ");
                String updateTime1 = entryDate1S[0] + " " + entryTime1;

                String entryDate2 = newsInfoBean2.getEntryDate();
                String entryTime2 = newsInfoBean2.getEntryTime();
                String[] entryDate2S = entryDate2.split(" ");
                String updateTime2 = entryDate2S[0] + " " + entryTime2;

                //Date date1 = DateUtils.str2Date(newsInfoBean1.getUpdateTime());
                //Date date2 = DateUtils.str2Date(newsInfoBean2.getUpdateTime());
                Date date1 = DateUtils.str2Date(updateTime1);
                Date date2 = DateUtils.str2Date(updateTime2);


                if (date1.after(date2) || date1.equals(date2)) {
                    LOG.debug("根据时间去重正确：NewsId-->SameNewsId: " + newsId + "-->" + sameNewsId + ": OK!");
                } else {
                    //LOG.info("News Date: " + date1);
                    //LOG.info("SameNewsId Date: " + date2);
                    LOG.error("两者的ISCD均为0，根据时间判断两者是否符合去重逻辑！" + "NewsId(" + date1 + ")(ISCD: " + iscd1 + ")-->SameNewsId(" + date2 + ")(ISCD: " + iscd2 + "): " + newsId + "-->" + sameNewsId + ": ERROR!");
                }

                if (date1.after(date2) || date1.equals(date2)) {
                    LOG.debug("根据时间去重正确：NewsId-->SameNewsId: " + newsId + "-->" + sameNewsId + ": OK!");
                } else {
                    //LOG.info("News Date: " + date1);
                    //LOG.info("SameNewsId Date: " + date2);
                    LOG.error("两者的ISCD均不为0，它们的优先级均是一样，根据它们的入库时间进行去重：" + "NewsId(" + date1 + ")(priority: " + priority1 + "; ISCD: " + iscd1 + ")-->SameNewsId(" + date2 + ")(priority: " + priority2 + "; ISCD: " + iscd2 + "): " + newsId + "-->" + sameNewsId + ": ERROR!");
                }
            } else {
                LOG.error("两者的优先级不一样，NewsId拥有较高的优先级，SameNewsId拥有较低的优先级：" + "NewsId(priority: " + priority1 + "; ISCD: " + iscd1 + ")-->SameNewsId(priority: " + priority2 + "; ISCD: " + iscd2 + "): " + newsId + "-->" + sameNewsId + ": ERROR!");
            }
        }
    }

    /**
     * @param groupName
     * @param siteName
     * @param channel
     * @return
     */
    private String getKey(String groupName, String siteName, String channel) {
        return groupName + ":" + siteName + ":" + channel;
    }


    /**
     * 根据新闻的ID获取新闻的时间
     *
     * @param newsId
     * @return
     * @throws SQLException
     */
    private Date getPubDate(int newsId) throws SQLException {
        Date date = null;
        pstmt1.setInt(1, newsId / 2);
        rs = pstmt1.executeQuery();
        while (rs.next()) {
            String pubDate = rs.getString(1);
            date = DateUtils.str2Date(pubDate);
        }
        return date;
    }

    private NewsInfoBean getNewsInfo(int newsId) {
        NewsInfoBean newsInfoBean = new NewsInfoBean();
        try {
            pstmt1.setInt(1, newsId);
            rs = pstmt1.executeQuery();
            while (rs.next()) {
                int slevel = rs.getInt(1);
                String updateTime = rs.getString(2);
                newsInfoBean.setNewsCode(newsId);
                newsInfoBean.setSlevel(slevel);
                newsInfoBean.setUpdateTime(updateTime);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return newsInfoBean;
    }

    public void updateNW0001() {
        String uSql = "update NW0001 set ISCD = 0";
        try {
            connToSQLServer();
            createStmt();
            LOG.info("正在将表NW0001中的ISCD全部设置为0......");
            stmt.execute(uSql);
            LOG.info("表NW0001中的ISCD已经全部更新为0！");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void updateUrlContent() {
        String uSql = "update URLCONTENT set ISEXPORT = 0";
        try {
            connToSQLServer();
            createStmt();
            LOG.info("正在将表URLCONTENT中的ISEXPORT全部设置为0......");
            stmt.execute(uSql);
            LOG.info("表URLCONTENT中的ISEXPORT已经全部更新为0！");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void checkNewData() {
        String sSql = "select top 100 sid from urlcontent with(nolock,index=IDX_ISEXPORT) where isexport=0 and (PKEY<>'0' or (pkey='0' and bbsnum>0)) order by sid asc";
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        DBUtils dbUtils = new DBUtils();
        dbUtils.setDbName("BasicDB_test");
        //dbUtils.selectSameNews();


        dbUtils.selectSameNewsP();
        dbUtils.selectNw2011();
        dbUtils.selectNw0001();
        dbUtils.priorityDupNews();


//        dbUtils.deleteData("NW0019_MQLog");
//
//        dbUtils.deleteData("SAMENEWS_P");
//        dbUtils.deleteData("SAMENEWS");
//        dbUtils.deleteData("NW2011");
//        dbUtils.deleteData("NW0001");
//
//        dbUtils.setDbName("TRS2");
//        dbUtils.updateUrlContent();
//        dbUtils.setDbName("TRS1");
//        dbUtils.updateUrlContent();
    }
}