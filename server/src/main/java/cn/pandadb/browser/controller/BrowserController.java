package cn.pandadb.browser.controller;

import java.util.Map;

import javax.annotation.Resource;

import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.pandadb.browser.VO.ExecuteCypherVo;
import cn.pandadb.browser.service.BrowserService;

@RestController
@RequestMapping("/pandadb/browser")
public class BrowserController {

    @Resource
    private BrowserService browserService;

    @PostMapping("/executeCypher")
    public Map<String, Object> executeCypher(ExecuteCypherVo executeCypherVo) {
        //executeCypherVo.setPandadbUrl("panda://10.0.82.148:7602");
        executeCypherVo.setPandadbUrl("panda://10.0.82.143:7612");
        if (StringUtils.isEmpty(executeCypherVo.getUsername())) {
            executeCypherVo.setUsername("neo4j");

        }
        if (StringUtils.isEmpty(executeCypherVo.getPassword())) {
            executeCypherVo.setPassword("neo4j");

        }
        return browserService.executeCypher(executeCypherVo);
    }

    @PostMapping("/getOtherRelationByNodeId")
    public Map<String, Object> getOtherRelationByNodeId(ExecuteCypherVo executeCypherVo) {
        executeCypherVo.setPandadbUrl("panda://10.0.82.148:7602");
        executeCypherVo.setPandadbUrl("panda://10.0.82.143:7612");
        if (StringUtils.isEmpty(executeCypherVo.getUsername())) {
            executeCypherVo.setUsername("neo4j");

        }
        if (StringUtils.isEmpty(executeCypherVo.getPassword())) {
            executeCypherVo.setPassword("neo4j");

        }
        return browserService.getOtherRelationByNodeId(executeCypherVo);
    }

    @PostMapping("/login")
    public Map<String, Object> login(ExecuteCypherVo executeCypherVo) {
        executeCypherVo.setPandadbUrl("panda://10.0.82.148:7602");
        executeCypherVo.setPandadbUrl("panda://10.0.82.143:7612");
        if (StringUtils.isEmpty(executeCypherVo.getUsername())) {
            executeCypherVo.setUsername("neo4j");

        }
        if (StringUtils.isEmpty(executeCypherVo.getPassword())) {
            executeCypherVo.setPassword("neo4j");

        }
        return browserService.getStatistics(executeCypherVo);
    }
}
