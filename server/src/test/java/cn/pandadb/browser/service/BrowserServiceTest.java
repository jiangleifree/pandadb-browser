package cn.pandadb.browser.service;

import java.util.Map;

import cn.pandadb.browser.VO.PandadbConnectionInfo;
import cn.pandadb.browser.utils.PatternProcess;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSONObject;

import cn.pandadb.browser.PandadbBrowserApplication;
import cn.pandadb.browser.VO.ExecuteCypherVo;
import lombok.extern.slf4j.Slf4j;

@SpringBootTest(classes = PandadbBrowserApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public class BrowserServiceTest {
    @Autowired
    private BrowserService browserService;

    @Test
    public void getDataTestV07() {
        String pandadbUrl = "panda://10.0.82.148:7602";
        int port = Integer.parseInt(pandadbUrl.split(":")[2]);
        String host = pandadbUrl.split(":")[1].substring(2);
        System.out.println(port);
        System.out.println(host);

        ExecuteCypherVo executeCypherVo = new ExecuteCypherVo();
        executeCypherVo.setPandadbUrl(pandadbUrl);
        executeCypherVo.setUsername("neo4j");
        executeCypherVo.setPassword("bigdata");
        executeCypherVo.setCypher("match (n) return n limit 11");
        PandadbConnectionInfo info = new PandadbConnectionInfo(executeCypherVo);

        //PandadbQueryTool pandadbQueryTool = new PandadbQueryTool(info);

        Map<String, Object> dataByCypher = PatternProcess.getDataByCypher(info);

        //pandadbQueryTool.getDataByCql("match (n) return n limit 1");
        //Map<String, Object> dataByCql = pandadbQueryTool.getDataByCql("match (n)-[r]-(n2) where id(n) = 300000003184020 return n,r,n2 limit 25");
        String s = JSONObject.toJSONString(dataByCypher);
        System.out.println(s);
    }

    @Test
    public void main() {
        String pandadbUrl = "panda://223.193.3.237:7600";
        int port = Integer.parseInt(pandadbUrl.split(":")[2]);
        String host = pandadbUrl.split(":")[1].substring(2);
        System.out.println(port);
        System.out.println(host);

        ExecuteCypherVo executeCypherVo = new ExecuteCypherVo();
        executeCypherVo.setPandadbUrl(pandadbUrl);
        executeCypherVo.setUsername("neo4j");
        executeCypherVo.setPassword("bigdata");
        executeCypherVo.setCypher("match (n)-[r]-(n2) where id(n) = 300000003184020 return n,r,n2 limit 25");
        PandadbConnectionInfo info = new PandadbConnectionInfo(executeCypherVo);

        //PandadbQueryTool pandadbQueryTool = new PandadbQueryTool(info);

        Map<String, Object> dataByCypher = PatternProcess.getDataByCypher(info);

        //pandadbQueryTool.getDataByCql("match (n) return n limit 1");
        //Map<String, Object> dataByCql = pandadbQueryTool.getDataByCql("match (n)-[r]-(n2) where id(n) = 300000003184020 return n,r,n2 limit 25");
        System.out.println(dataByCypher);
    }

    @Test
    public void getDataByCypherTest() {
        ExecuteCypherVo executeCypherVo = new ExecuteCypherVo();
        executeCypherVo.setPandadbUrl("panda://xxx:xx");
        executeCypherVo.setUsername("");
        executeCypherVo.setPassword("");
        executeCypherVo.setCypher("match (n:person)  return n");
        //executeCypherVo.setCypher("match (n:person) where n.name='google' return n");

        Map<String, Object> result =
                browserService.executeCypher(executeCypherVo);

        JSONObject jsonObject = new JSONObject(result);
        log.info(jsonObject.toJSONString());
    }

    @Test
    public void insertTest() {
        String cyphe = "create (n:person) set n.name = 'jl'";

        ExecuteCypherVo executeCypherVo = new ExecuteCypherVo();
        executeCypherVo.setPandadbUrl("10.0.82.148:9989");
        executeCypherVo.setUsername("");
        executeCypherVo.setPassword("");
        executeCypherVo.setCypher(cyphe);
        //executeCypherVo.setCypher("match (n:person) where n.name='google' return n");

        Map<String, Object> result =
                browserService.executeCypher(executeCypherVo);

        JSONObject jsonObject = new JSONObject(result);
        log.info(jsonObject.toJSONString());
    }


    @Test
    public void getStatisticsTest() {
        ExecuteCypherVo executeCypherVo = new ExecuteCypherVo();
        executeCypherVo.setPandadbUrl("panda://xxxx:xx");
        executeCypherVo.setUsername("");
        executeCypherVo.setPassword("");
        executeCypherVo.setCypher("match (n:person) where n.name='google' return n");
        Map<String, Object> result = browserService.getStatistics(executeCypherVo);
        JSONObject jsonObject = new JSONObject(result);
        log.info(jsonObject.toJSONString());
    }
}
