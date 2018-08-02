package com.jd.ad.anti.cps.entity;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.List;

import com.alibaba.fastjson.JSON;

public class CpsRelatedResult {
    
    ClickInfo clickInfo;
    
    public void Parse(String cpsRelatedResult) throws UnsupportedEncodingException {
        String decodedResult = URLDecoder.decode(cpsRelatedResult, "UTF-8");

        clickInfo = JSON.parseObject(decodedResult, ClickInfo.class);
        if (clickInfo == null) {
            throw new UnsupportedEncodingException("could not parse cpsRelatedResult, get null, "
                + decodedResult);
        }
    }

    public List<SkuClickInfo> getSkuClickInfo() {
        return clickInfo.getSku();
    }

    public OrderClickInfo getOrderClickInfo() {
         return clickInfo.getOrder();
    }

    public static class ClickInfo implements Serializable {

        private static final long serialVersionUID = 1L;
        private OrderClickInfo order;
        private List<SkuClickInfo> sku;

        public OrderClickInfo getOrder() {
            return order;
        }

        public void setOrder(OrderClickInfo order) {
            this.order = order;
        }

        public List<SkuClickInfo> getSku() {
            return sku;
        }

        public void setSku(List<SkuClickInfo> sku) {
            this.sku = sku;
        }
    }

    public static class OrderClickInfo implements Serializable {

        private static final long serialVersionUID = 1L;
        private Long orderid;
        private Integer related_result;// 0影响订单 1直接订单
        private String judge_law1;// adowner
        private CpsClickLog cps_click_log;
        private Integer jda_source;// 标识是否是跨屏订单，0非跨屏 1跨屏

        public Long getOrderid() {
            return orderid;
        }

        public void setOrderid(Long orderid) {
            this.orderid = orderid;
        }

        public Integer getRelated_result() {
            return related_result;
        }

        public void setRelated_result(Integer related_result) {
            this.related_result = related_result;
        }

        public CpsClickLog getCps_click_log() {
            return cps_click_log;
        }

        public void setCps_click_log(CpsClickLog cps_click_log) {
            this.cps_click_log = cps_click_log;
        }

        public String getJudge_law1() {
            return judge_law1;
        }

        public void setJudge_law1(String judge_law1) {
            this.judge_law1 = judge_law1;
        }

        public Integer getJda_source() {
            return jda_source;
        }

        public void setJda_source(Integer jda_source) {
            this.jda_source = jda_source;
        }

    }

    public static class SkuClickInfo implements Serializable {

        private static final long serialVersionUID = 1L;
        private Long sku_Id;// 商品skuID 用到
        private Long orderid;
        private String judge_law1;// adownerId 用到
        private Integer related_result;// //1是直接sku，0是影响sku 2同店
                                       // 3跨店（同店和跨店是做lastCookie需求时添加的 ） 用到
        private Integer my_self;// 吴健用来判断直接sku的字段，我们不需要关心
        private CpsClickLog cps_click_log;// cps 站外点击日志 用到
        private Integer jda_source;// 标识是否是跨屏，0非跨屏 1跨屏
        private Integer cat1;// 一级类目 用到
        private Integer cat2;// 二级类目 用到
        private Integer cat3;// 三级类目 用到
        private Long venderid;// 商家ID 用到
        private Long spu;// spu or wareid
        private CpsClickLog si_cps_click_log;// cps 站内点击日志 用到
        private Long act_unionid;// cps 站外文章对应点击联盟id 用到
        private Integer alloc_type;// 分佣类型 （1.单方分佣 2.站内和站外分佣 3.站外和文章分佣 ） 用到

        private CpsClickLog useCpsClickLog;// 计算佣金用到的点击 用到
        private CpsClickLog notUseCpsClickLog;// 计算佣金用不到的点击 用到

        public Long getSku_Id() {
            return sku_Id;
        }

        public void setSku_Id(Long sku_Id) {
            this.sku_Id = sku_Id;
        }

        public Long getOrderid() {
            return orderid;
        }

        public void setOrderid(Long orderid) {
            this.orderid = orderid;
        }

        public Integer getRelated_result() {
            return related_result;
        }

        public void setRelated_result(Integer related_result) {
            this.related_result = related_result;
        }

        public CpsClickLog getCps_click_log() {
            return cps_click_log;
        }

        public void setCps_click_log(CpsClickLog cps_click_log) {
            this.cps_click_log = cps_click_log;
        }

        public String getJudge_law1() {
            return judge_law1;
        }

        public void setJudge_law1(String judge_law1) {
            this.judge_law1 = judge_law1;
        }

        public Integer getJda_source() {
            return jda_source;
        }

        public void setJda_source(Integer jda_source) {
            this.jda_source = jda_source;
        }

        public Integer getMy_self() {
            return my_self;
        }

        public void setMy_self(Integer my_self) {
            this.my_self = my_self;
        }

        public CpsClickLog getSi_cps_click_log() {
            return si_cps_click_log;
        }

        public void setSi_cps_click_log(CpsClickLog si_cps_click_log) {
            this.si_cps_click_log = si_cps_click_log;
        }

        public Long getAct_unionid() {
            return act_unionid;
        }

        public void setAct_unionid(Long act_unionid) {
            this.act_unionid = act_unionid;
        }

        public Integer getAlloc_type() {
            return alloc_type;
        }

        public void setAlloc_type(Integer alloc_type) {
            this.alloc_type = alloc_type;
        }

        public CpsClickLog getUseCpsClickLog() {
            return useCpsClickLog;
        }

        public void setUseCpsClickLog(CpsClickLog useCpsClickLog) {
            this.useCpsClickLog = useCpsClickLog;
        }

        public CpsClickLog getNotUseCpsClickLog() {
            return notUseCpsClickLog;
        }

        public void setNotUseCpsClickLog(CpsClickLog notUseCpsClickLog) {
            this.notUseCpsClickLog = notUseCpsClickLog;
        }

        public Integer getCat1() {
            return cat1;
        }

        public void setCat1(Integer cat1) {
            this.cat1 = cat1;
        }

        public Integer getCat2() {
            return cat2;
        }

        public void setCat2(Integer cat2) {
            this.cat2 = cat2;
        }

        public Integer getCat3() {
            return cat3;
        }

        public void setCat3(Integer cat3) {
            this.cat3 = cat3;
        }

        public Long getVenderid() {
            return venderid;
        }

        public void setVenderid(Long venderid) {
            this.venderid = venderid;
        }

        public Long getSpu() {
            return spu;
        }

        public void setSpu(Long spu) {
            this.spu = spu;
        }
    }

    public static class CpsClickLog implements Serializable {

        private static final long serialVersionUID = 1L;
        private Integer nc; // 作弊标示 用到
        private Date createtime;// 点击时间 用到
        private String ip;//
        private String clickId; // 用到
        private String pin;// 用户pin
        private Integer caseId;// js橱窗ID
        private Long proId;// 推广类型 用到对应的是sptype
        private Long planId;// 计划ID
        private Long matId;// js素材ID
        private String st;// 点击来源
        private String tu;// 目标页
        private String playId;// js播放ID
        private Integer siteId;// 网站ID 用到, change Long to Integer by hejingjiang@jd.com
        private Integer unionId;// 联盟ID 用到, change Long to Integer by hejingjiang@jd.com
        private String cUnionId;// 子联盟ID 用到
        private String webtype;// 网络类型 用到 对应的是source_emt
        private String euid;// 子子联盟ID,站外用户设置 用到 子子联盟ID
        private String adOwner;// 广告推广者
        private String sku;// 落地单品页sku
        private String jdaUid; // jda中用户唯一ID 用到 用到 jda
        private String unpl;// 用户unpl
        private Integer convertType;// 转化目标页类型(PC端商品页转到re.jd.com的类型:1,默认不转化为0)
        private Integer adTrafficType;// cps cpc 微店 用到 adtraffictype
        private Long adId;// 广告位id 用于播放 用到 对应ad_id
        private Integer positionId; // 联盟推广位id 站长账户结构 用到 对应sp_id, change by hejingjiang@jd.com
        private Long actId;// 活动ID 和 文章ID 用到 对应 act_id
        private Long adSpreadType;// 站内/站外/页面 ,1-站内，2-站外， 3-中间页/流量池 用到
                                  // 对应spread_type

        private Integer unionAdtType;// 新增映射后adttype 用到 对应union_traffic_type
        private Integer pro_type;// 映射后推广方式（自定义，非自定义，js） 用到 对应promotion_type
        private String pro_cont;// 映射后推广内容 用到 对应promotion_cont
        private Integer platform;

        public Integer getNc() {
            return nc;
        }

        public void setNc(Integer nc) {
            this.nc = nc;
        }

        public Date getCreatetime() {
            return createtime;
        }

        public void setCreatetime(Date createtime) {
            this.createtime = createtime;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getClickId() {
            return clickId;
        }

        public void setClickId(String clickId) {
            this.clickId = clickId;
        }

        public String getPin() {
            return pin;
        }

        public void setPin(String pin) {
            this.pin = pin;
        }

        public Integer getCaseId() {
            return caseId;
        }

        public void setCaseId(Integer caseId) {
            this.caseId = caseId;
        }

        public Long getProId() {
            return proId;
        }

        public void setProId(Long proId) {
            this.proId = proId;
        }

        public Long getPlanId() {
            return planId;
        }

        public void setPlanId(Long planId) {
            this.planId = planId;
        }

        public Long getMatId() {
            return matId;
        }

        public void setMatId(Long matId) {
            this.matId = matId;
        }

        public String getSt() {
            return st;
        }

        public void setSt(String st) {
            this.st = st;
        }

        public String getTu() {
            return tu;
        }

        public void setTu(String tu) {
            this.tu = tu;
        }

        public String getPlayId() {
            return playId;
        }

        public void setPlayId(String playId) {
            this.playId = playId;
        }

        public Integer getSiteId() {
            return siteId;
        }

        public void setSiteId(Integer siteId) {
            this.siteId = siteId;
        }

        public Integer getUnionId() {
            return unionId;
        }

        public void setUnionId(Integer unionId) {
            this.unionId = unionId;
        }

        public String getCUnionId() {
            return cUnionId;
        }

        public void setCUnionId(String cUnionId) {
            this.cUnionId = cUnionId;
        }

        public String getWebtype() {
            return webtype;
        }

        public void setWebtype(String webtype) {
            this.webtype = webtype;
        }

        public String getEuid() {
            return euid;
        }

        public void setEuid(String euid) {
            this.euid = euid;
        }

        public String getAdOwner() {
            return adOwner;
        }

        public void setAdOwner(String adOwner) {
            this.adOwner = adOwner;
        }

        public String getSku() {
            return sku;
        }

        public void setSku(String sku) {
            this.sku = sku;
        }

        public String getJdaUid() {
            return jdaUid;
        }

        public void setJdaUid(String jdaUid) {
            this.jdaUid = jdaUid;
        }

        public String getUnpl() {
            return unpl;
        }

        public void setUnpl(String unpl) {
            this.unpl = unpl;
        }

        public Integer getConvertType() {
            return convertType;
        }

        public void setConvertType(Integer convertType) {
            this.convertType = convertType;
        }

        public Integer getAdTrafficType() {
            return adTrafficType;
        }

        public void setAdTrafficType(Integer adTrafficType) {
            this.adTrafficType = adTrafficType;
        }

        public Long getAdId() {
            return adId;
        }

        public void setAdId(Long adId) {
            this.adId = adId;
        }

        public Integer getPositionId() {
            return positionId;
        }

        public void setPositionId(Integer positionId) {
            this.positionId = positionId;
        }

        public String getcUnionId() {
            return cUnionId;
        }

        public void setcUnionId(String cUnionId) {
            this.cUnionId = cUnionId;
        }

        public Integer getUnionAdtType() {
            return unionAdtType;
        }

        public void setUnionAdtType(Integer unionAdtType) {
            this.unionAdtType = unionAdtType;
        }

        public Integer getPro_type() {
            return pro_type;
        }

        public void setPro_type(Integer pro_type) {
            this.pro_type = pro_type;
        }

        public String getPro_cont() {
            return pro_cont;
        }

        public void setPro_cont(String pro_cont) {
            this.pro_cont = pro_cont;
        }

        public Long getActId() {
            return actId;
        }

        public void setActId(Long actId) {
            this.actId = actId;
        }

        public Long getAdSpreadType() {
            return adSpreadType;
        }

        public void setAdSpreadType(Long adSpreadType) {
            this.adSpreadType = adSpreadType;
        }
        
        public Integer getPlatform() {
            return platform;
        }

        public void setPlatform(Integer platform) {
            this.platform = platform;
        }
    }
}
