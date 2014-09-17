CREATE TABLE campaignData(campaignId String,campaignName String,adgroupId String,adgroupName String,keywordId String,keyword String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH '/tmp/campaignData.txt' INTO TABLE campaignData;