package com.to8to.UserLogBaiDuSEM;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.drapi.autosdk.core.CommonService;
import com.baidu.drapi.autosdk.core.ResHeader;
import com.baidu.drapi.autosdk.core.ResHeaderUtil;
import com.baidu.drapi.autosdk.core.ServiceFactory;
import com.baidu.drapi.autosdk.exception.ApiException;
import com.baidu.drapi.autosdk.sms.v3.BulkJobService;
import com.baidu.drapi.autosdk.sms.v3.FilePathType;
import com.baidu.drapi.autosdk.sms.v3.GetAllObjectsRequest;
import com.baidu.drapi.autosdk.sms.v3.GetAllObjectsResponse;
import com.baidu.drapi.autosdk.sms.v3.GetFilePathRequest;
import com.baidu.drapi.autosdk.sms.v3.GetFilePathResponse;
import com.baidu.drapi.autosdk.sms.v3.GetFileStateRequest;
import com.baidu.drapi.autosdk.sms.v3.GetFileStateResponse;
import com.to8to.commons.utils.Config;

public class GetCampaignService
{

    public static Logger              logger     = LoggerFactory
                                                         .getLogger(GetCampaignService.class);

    public static Map<String, String> campMap    = new HashMap<String, String>();

    public static Map<String, String> adgroupMap = new HashMap<String, String>();

    public static void getCampMap(String configurl)
    {
        Config config = new Config(configurl);

        String campaignlocal = config.get("campaignlocal");

        File file = new File(campaignlocal);

        try
        {
            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), "GBK");
            BufferedReader reader = new BufferedReader(read);
            String line = reader.readLine();
            while ((line = reader.readLine()) != null)
            {
                String[] dataArray = line.split("\t");
                campMap.put(dataArray[0], dataArray[1]);
            }
            reader.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public static void getAdgroupMap(String configurl)
    {

        Config config = new Config(configurl);
        String adgrouplocal = config.get("adgrouplocal");
        File file = new File(adgrouplocal);

        try
        {
            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), "GBK");
            BufferedReader reader = new BufferedReader(read);
            String line = reader.readLine();
            while ((line = reader.readLine()) != null)
            {
                String[] dataArray = line.split("\t");
                adgroupMap.put(dataArray[1], dataArray[2]);
            }
            reader.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void makeFile(String configurl)
    {
        Config config = new Config(configurl);

        String keywordlocal = config.get("keywordlocal");

        String keywordlocaltxt = config.get("keywordlocaltxt");

        File file = new File(keywordlocal);
        try
        {
            InputStreamReader read = new InputStreamReader(new FileInputStream(
                    file), "GBK");

            BufferedReader reader = new BufferedReader(read);

            FileWriter writer = new FileWriter(keywordlocaltxt, true);

            String line = reader.readLine();

            StringBuffer sbffer = new StringBuffer();

            while ((line = reader.readLine()) != null)
            {
                String[] dataArray = line.split("\t");
                String campaignId = dataArray[0];
                String adgroupId = dataArray[1];
                String keywordId = dataArray[2];
                String keyword = dataArray[3];
                sbffer.append(campaignId).append("\t")
                        .append(campMap.get(campaignId)).append("\t")
                        .append(adgroupId).append("\t")
                        .append(adgroupMap.get(adgroupId)).append("\t")
                        .append(keywordId).append("\t").append(keyword)
                        .append("\n");
            }
            writer.write(sbffer.toString());
            writer.flush();
            writer.close();
            reader.close();
            read.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void file2Hive(String yesterday) throws Exception
    {
        Config config = new Config("hive.properties");
        String hive_jdbc_url = config.get("hive_jdbc_url");
        String driver_name = config.get("driver_name");
        Class.forName(driver_name);
        Connection conn = DriverManager.getConnection(hive_jdbc_url);
        Statement stmt = conn.createStatement();
        String filepath = config.get("hdfs_file_path");
        String sql = "load data local inpath '" + filepath
                + "' overwrite into table sem_campaigndata PARTITION (dt="
                + yesterday + ")";
        stmt.execute(sql);
    }

    public static void downloadFile(String downloadURL, String localPath)
            throws Exception
    {
        HttpClient httpClient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(downloadURL);
        HttpResponse httpResponse = httpClient.execute(httpGet);
        StatusLine statusLine = httpResponse.getStatusLine();
        if (statusLine.getStatusCode() == 200)
        {
            File xml = new File(localPath);
            FileOutputStream outputStream = new FileOutputStream(xml);
            InputStream inputStream = httpResponse.getEntity().getContent();
            byte buff[] = new byte[4096];
            int counts = 0;
            while ((counts = inputStream.read(buff)) != -1)
            {
                outputStream.write(buff, 0, counts);
            }
            outputStream.flush();
            outputStream.close();
        }
        httpClient.getConnectionManager().shutdown();
    }

    public static void getCampinData(String configurl) throws Exception
    {
        try
        {

            Config config = new Config(configurl);

            String serverurl = config.get("serverurl");
            String username = config.get("username");
            username = new String(username.getBytes("ISO-8859-1"), "UTF-8");
            String password = config.get("password");
            String token = config.get("token");

            String campaignlocalgz = config.get("campaignlocalgz");
            String adgrouplocalgz = config.get("adgrouplocalgz");
            String keywordlocalgz = config.get("keywordlocalgz");
            String campaignlocal = config.get("campaignlocal");
            String adgrouplocal = config.get("adgrouplocal");
            String keywordlocal = config.get("keywordlocal");

            CommonService factory = ServiceFactory.getInstance();
            factory.setServerUrl(serverurl);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setToken(token);

            BulkJobService service = factory.getService(BulkJobService.class);
            GetAllObjectsRequest getAllObjectsRequest = new GetAllObjectsRequest();
            GetAllObjectsResponse res = service
                    .getAllObjects(getAllObjectsRequest);

            ResHeader rheader = ResHeaderUtil.getResHeader(service, true);
            String aFileId = res.getFileId();

            GetFileStateRequest getFileStateRequest = new GetFileStateRequest();
            getFileStateRequest.setFileId(aFileId);

            GetFileStateResponse getFileStateResponse = service
                    .getFileState(getFileStateRequest);

            boolean flag = true;

            while (flag)
            {
                if (getFileStateResponse.getIsGenerated() == 3)
                {

                    GetFilePathRequest getFilePathRequest = new GetFilePathRequest();
                    getFilePathRequest.setFileId(aFileId);
                    GetFilePathResponse getFilePathResponse = service
                            .getFilePath(getFilePathRequest);
                    FilePathType filePathType = getFilePathResponse
                            .getFilePaths();

                    String campaignFilePath = filePathType
                            .getCampaignFilePath();

                    logger.debug("campaignFilePath: " + campaignFilePath);

                    String adgroupFilePath = filePathType.getAdgroupFilePath();

                    logger.debug("adgroupFilePath: " + adgroupFilePath);

                    String keywordFilePath = filePathType.getKeywordFilePath();

                    logger.debug("keywordFilePath: " + keywordFilePath);

                    downloadFile(campaignFilePath, campaignlocalgz);

                    downloadFile(adgroupFilePath, adgrouplocalgz);

                    downloadFile(keywordFilePath, keywordlocalgz);

                    doUncompressFile(campaignlocalgz, campaignlocal);

                    doUncompressFile(adgrouplocalgz, adgrouplocal);

                    doUncompressFile(keywordlocalgz, keywordlocal);

                    getCampMap(configurl);

                    getAdgroupMap(configurl);

                    makeFile(configurl);

                    flag = false;

                }
                else
                {
                    Thread.currentThread().sleep(20000);
                    getFileStateResponse = service
                            .getFileState(getFileStateRequest);
                    System.out
                            .println("getFileStateResponse.getIsGenerated(): "
                                    + getFileStateResponse.getIsGenerated());
                }
            }
        }
        catch (ApiException e)
        {
            e.printStackTrace();
        }
    }

    public static void doUncompressFile(String inFileName, String outFileName)
    {
        GZIPInputStream in = null;
        FileOutputStream out = null;
        try
        {
            in = new GZIPInputStream(new FileInputStream(inFileName));
            out = new FileOutputStream(outFileName);
            byte[] buf = new byte[1024];
            int len;
            while ((len = in.read(buf)) > 0)
            {
                out.write(buf, 0, len);
            }
            in.close();
            out.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {

        try
        {

            String[] configurls =
            { "baidusem0.properties", "baidusem1.properties",
                    "baidusem2.properties" };

            for (int i = 0; i < configurls.length; i++)
            {
                getCampinData(configurls[i]);
            }
            
            String yestedayDate="";
            
            if(args.length==0)
            {
                Calendar calendar = Calendar.getInstance();// 此时打印它获取的是系统当前时间
                calendar.add(Calendar.DATE, -1); // 得到前一天
                yestedayDate = new SimpleDateFormat("yyyyMMdd")
                        .format(calendar.getTime());
                logger.debug("yestedayDate: "+yestedayDate);
            }
            else
            {
                yestedayDate=args[0];
                logger.debug("yestedayDate 2: "+yestedayDate);
            }

            file2Hive(yestedayDate);
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

}
