package aifunai.model;

import java.util.Date;

public class TestTable {
    private Long id;

    private Byte status;

    private Float lng;

    private Float lat;

    private Float score;

    private Integer areaId;

    private Integer countryId;

    private String tags;

    private String poiNameCn;

    private String poiNameEn;

    private String poiNameLocal;

    private String poiDetails;

    private String openTime;

    private String poiTel;

    private String poiUrl;

    private String arrivalPattern;

    private Float visitingTime;

    private Integer price;

    private String tips;

    private String picUrls;

    private Integer remarkCount;

    private String addressCn;

    private String addressLocal;

    private String addressEn;

    private String extendFields;

    private Date gmtCreate;

    private Date gmtModify;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Byte getStatus() {
        return status;
    }

    public void setStatus(Byte status) {
        this.status = status;
    }

    public Float getLng() {
        return lng;
    }

    public void setLng(Float lng) {
        this.lng = lng;
    }

    public Float getLat() {
        return lat;
    }

    public void setLat(Float lat) {
        this.lat = lat;
    }

    public Float getScore() {
        return score;
    }

    public void setScore(Float score) {
        this.score = score;
    }

    public Integer getAreaId() {
        return areaId;
    }

    public void setAreaId(Integer areaId) {
        this.areaId = areaId;
    }

    public Integer getCountryId() {
        return countryId;
    }

    public void setCountryId(Integer countryId) {
        this.countryId = countryId;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags == null ? null : tags.trim();
    }

    public String getPoiNameCn() {
        return poiNameCn;
    }

    public void setPoiNameCn(String poiNameCn) {
        this.poiNameCn = poiNameCn == null ? null : poiNameCn.trim();
    }

    public String getPoiNameEn() {
        return poiNameEn;
    }

    public void setPoiNameEn(String poiNameEn) {
        this.poiNameEn = poiNameEn == null ? null : poiNameEn.trim();
    }

    public String getPoiNameLocal() {
        return poiNameLocal;
    }

    public void setPoiNameLocal(String poiNameLocal) {
        this.poiNameLocal = poiNameLocal == null ? null : poiNameLocal.trim();
    }

    public String getPoiDetails() {
        return poiDetails;
    }

    public void setPoiDetails(String poiDetails) {
        this.poiDetails = poiDetails == null ? null : poiDetails.trim();
    }

    public String getOpenTime() {
        return openTime;
    }

    public void setOpenTime(String openTime) {
        this.openTime = openTime == null ? null : openTime.trim();
    }

    public String getPoiTel() {
        return poiTel;
    }

    public void setPoiTel(String poiTel) {
        this.poiTel = poiTel == null ? null : poiTel.trim();
    }

    public String getPoiUrl() {
        return poiUrl;
    }

    public void setPoiUrl(String poiUrl) {
        this.poiUrl = poiUrl == null ? null : poiUrl.trim();
    }

    public String getArrivalPattern() {
        return arrivalPattern;
    }

    public void setArrivalPattern(String arrivalPattern) {
        this.arrivalPattern = arrivalPattern == null ? null : arrivalPattern.trim();
    }

    public Float getVisitingTime() {
        return visitingTime;
    }

    public void setVisitingTime(Float visitingTime) {
        this.visitingTime = visitingTime;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public String getTips() {
        return tips;
    }

    public void setTips(String tips) {
        this.tips = tips == null ? null : tips.trim();
    }

    public String getPicUrls() {
        return picUrls;
    }

    public void setPicUrls(String picUrls) {
        this.picUrls = picUrls == null ? null : picUrls.trim();
    }

    public Integer getRemarkCount() {
        return remarkCount;
    }

    public void setRemarkCount(Integer remarkCount) {
        this.remarkCount = remarkCount;
    }

    public String getAddressCn() {
        return addressCn;
    }

    public void setAddressCn(String addressCn) {
        this.addressCn = addressCn == null ? null : addressCn.trim();
    }

    public String getAddressLocal() {
        return addressLocal;
    }

    public void setAddressLocal(String addressLocal) {
        this.addressLocal = addressLocal == null ? null : addressLocal.trim();
    }

    public String getAddressEn() {
        return addressEn;
    }

    public void setAddressEn(String addressEn) {
        this.addressEn = addressEn == null ? null : addressEn.trim();
    }

    public String getExtendFields() {
        return extendFields;
    }

    public void setExtendFields(String extendFields) {
        this.extendFields = extendFields == null ? null : extendFields.trim();
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModify() {
        return gmtModify;
    }

    public void setGmtModify(Date gmtModify) {
        this.gmtModify = gmtModify;
    }
}