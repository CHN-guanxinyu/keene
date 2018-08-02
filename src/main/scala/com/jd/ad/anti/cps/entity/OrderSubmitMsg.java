package com.jd.ad.anti.cps.entity;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jd.purchase.domain.old.bean.SKU;
import com.jd.purchase.domain.old.bean.Suit;
import com.jd.purchase.sdk.common.serialize.XmlSerializableTool;

public class OrderSubmitMsg implements Serializable {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private static final Log logger = LogFactory.getLog(OrderSubmitMsg.class);
    private String orderAndCartSplit = "8e5220aa_bbc2_4eb9_b849_a4afd76bc6e3";
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private com.jd.purchase.domain.old.bean.Order orderMq = null;
    private com.jd.purchase.domain.old.bean.Cart cartMq = null;
    private CpsRelatedResult cpsRelatedResult = null;

    public void parse(String xmlString) throws Exception {

        String[] orderAndCart = xmlString.split(orderAndCartSplit, 2);
        if (orderAndCart != null && orderAndCart.length != 2) {
            throw new Exception("invalid cps order mq: " + xmlString);
        }
        orderMq = XmlSerializableTool.deSerializeXML(com.jd.purchase.domain.old.bean.Order.class,
            orderAndCart[0], true);
//        cartMq = XmlSerializableTool.deSerializeXML(com.jd.purchase.domain.old.bean.Cart.class,
//            orderAndCart[1], true);
        this.parseCpsRelatedResult();
    }

    public long getOrderId() {
        return this.orderMq.getOrderId();
    }

    public long getParentId() {
        return this.orderMq.getParentId();
    }
    
    public int getCreateDate() {
        String literalCreateDate = null;
        Date createDate = null;
        int seconds = 0;
        try {
            literalCreateDate = this.orderMq.getCreateDate();
            if (literalCreateDate != null) {
                createDate = dateFormat.parse(literalCreateDate.replace("T", " "));
                if (createDate != null) {
                    // transform to seconds
                    seconds = (int) Math.floor(createDate.getTime() / 1000);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return seconds;
    }

    
    public long getUserId() {
        return this.orderMq.getUserId();
    }
    
    public int getUserLevel() {
        return this.orderMq.getUserLevel();
    }
    
    public int getIdProvince() {
        return this.orderMq.getIdProvince();
    }
    
    public int getIdCity() {
        return this.orderMq.getIdCity();
    }
    
    public int getCounty() {
        return this.orderMq.getCounty();
    }
    
    public int getIdShipmentType() {
        return this.orderMq.getIdShipmentType();
    }
    
    public boolean isIsJdShip() {
        return this.orderMq.isIsJdShip();
    }
    
    public int getPopVenderId() {
        return this.orderMq.getPop() != null ? this.orderMq.getPop().getVenderId() : -1;
    }
    
    public String getClientIP() {
        String ip =  this.orderMq.getClientIP();
        if (ip == null)
            ip ="";
        return ip;
    }
    
    public int getSkuNum() {
        int skuNum = 0;

        for (Entry<String, Suit> entry : this.cartMq.getPacks().entrySet()) {
            skuNum += entry.getValue().getSkus().size();
        }

        return skuNum;
    }

    public int getGoodsNum() {
        int goodsNum = 0;

        for (Entry<String, Suit> entry : this.cartMq.getPacks().entrySet()) {
            for (SKU sku : entry.getValue().getSkus())
                goodsNum += sku.getNum();
        }

        return goodsNum;
    }
    
    public long getDiscount() {
        return (long) (this.orderMq.getDiscount().doubleValue() * 100);
    }
    
    public long getPrice() {
        return (long) (this.orderMq.getPrice().doubleValue() * 100);
    }
    
    public long getTotalFee() {
        return (long) (this.orderMq.getTotalFee().doubleValue() * 100);
    }
    
    public CpsRelatedResult getCpsRelatedResult() {
        return cpsRelatedResult;
    }
    
    protected void parseCpsRelatedResult() throws UnsupportedEncodingException {
        Map<String, String> extTagMap = null;
        String cpsRelatedResultStr = null;
        
        extTagMap = this.orderMq.getExtTags();
        if (extTagMap != null) {
            cpsRelatedResultStr = extTagMap.get("CpsRelatedResult");
            if (cpsRelatedResultStr != null) {
                cpsRelatedResult = new CpsRelatedResult();
                cpsRelatedResult.Parse(cpsRelatedResultStr);
            }
        }
    }
    
}
