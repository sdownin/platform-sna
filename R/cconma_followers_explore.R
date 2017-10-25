##
# CCONMA FOLLOWERS RELATIONS DATA EXPLORATION
# SMJ SPECIAL ISSUE  FEB 2018
#
# cconma_order;
#   +---------------+---------------------+------+-----+---------------------+-------+
#   | Field         | Type                | Null | Key | Default             | Extra |
#   +---------------+---------------------+------+-----+---------------------+-------+
#   | ocode         | varchar(20)         | NO   | PRI |                     |       |
#   | seller_mem_no | int(10) unsigned    | NO   | MUL | 0                   |       |
#   | mem_no        | int(10) unsigned    | NO   | MUL | 0                   |       |
#   | order_date    | datetime            | NO   | MUL | 0000-00-00 00:00:00 |       |
#   | ip_addr       | varchar(15)         | YES  | MUL | NULL                |       |
#   | f_mobile      | enum('Y','N')       | NO   | MUL | N                   |       |
#   | device_type   | varchar(20)         | YES  |     | NULL                |       |
#   | agent_info    | varchar(100)        | YES  |     | NULL                |       |
#   | status        | tinyint(4) unsigned | NO   | MUL | 1                   |       |
#   | amount        | int(10) unsigned    | NO   | MUL | 0                   |       |
#   | dlvry_fee     | int(10) unsigned    | NO   | MUL | 0                   |       |
#   +---------------+---------------------+------+-----+---------------------+-------+
##

# library(RODBC)
library(RMySQL)
library(reshape2)
library(plyr)
library(igraph)
library(ggplot2)
library(lattice)
library(lubridate)
library(CausalImpact)
library(stringr)
library(zoo)


##
# Return a dataframe for the query string
##
fetch <- function(query) {
  con <- dbConnect(RMySQL::MySQL(), dbname = "cconma2", user="root", password="", port=3306)
  rs <- dbSendQuery(con, query)
  data <- dbFetch(rs, n= -1)
  dbDisconnect(con)
  return(data)
}

getMonthDays <- function(month) {
  return( 
    switch(as.character(month), 
           '1'= 31,'2'= 28,'3'= 31,'4'= 30,'5'= 31,'6'= 30,
           '7'= 31,'8'= 31,'9'= 30,'10'= 31,'11'= 30,'12'= 31
           )
    )
}

getYearMonthLastDate <- function(yearMonth, sep="-") {
  x <- as.numeric(str_split(yearMonth, sep)[[1]])
  return(as.Date(ISOdate(x[1],x[2],getMonthDays(x[2]))))
}

##_---------------------------------------------

# con <- odbcConnect(dsn = "cconma2", uid = "root", pwd = "")
con <- dbConnect(RMySQL::MySQL(), dbname = "cconma2", user="root", password="", port=3306)

q <- '
SELECT MONTH(order_date) month,
  YEAR(order_date) yr,
  DATE_FORMAT(order_date,"%Y-%m") pd,
  seller_mem_no,
  mem_no,
  SUM(amount / 1120) revenue
FROM cconma_order
GROUP BY pd;
'

rs <- dbSendQuery(con, q)
dfor <- dbFetch(rs, n= -1)
dbDisconnect(con)

dfor$t <- as.numeric(as.factor(dfor$pd))
dim(dfor)
head(dfor)

qplot(x = period, y=rev, data=dfor) 

follow.start <- which(dfor$pd == '2016-02')

ggplot(aes(x=t, y=revenue), data=dfor) + 
  geom_vline(xintercept=follow.start, col='steelblue',lwd=1.5) +
  geom_point() + geom_line() + 
  ylim(low=0,high=max(dfor$revenue)) + 
  ggtitle("Monthly Total Revenue (USD)") +
  theme_bw()

# #--------------------------------------------------------
# 
# con <- dbConnect(RMySQL::MySQL(), dbname = "cconma2", user="root", password="", port=3306)
# 
# q <- '
# SELECT mem_no,
# MONTH(order_date) month,
# YEAR(order_date) yr,
# DATE_FORMAT(order_date,"%Y-%m") pd,
# SUM(amount) rev_krw_sum,
# AVG(amount) rev_krw_avg,
# STD(amount) rev_krw_std,
# (CASE WHEN mem_no IN (SELECT DISTINCT follower_mem_no FROM cconma_memberfollow) THEN 1 ELSE 0 END) is_follower,
# (CASE WHEN mem_no IN (SELECT DISTINCT following_mem_no FROM cconma_memberfollow) THEN 1 ELSE 0 END) is_following
# FROM cconma_order
# GROUP BY pd,mem_no
# ORDER BY pd asc;
# '
# 
# rs <- dbSendQuery(con, q)
# df2 <- dbFetch(rs, n= -1)
# dbDisconnect(con)
# 
# df2$is_both <- df2$is_follower * df2$is_following
# df2$t <- as.numeric(as.factor(df2$pd))
# df2$period <- lubridate::ymd(df2$pd)
# 
# dim(df2)
# head(df2)
# 
# df2f <- plyr::ddply(df2, .variables = c('pd','is_follower'),summarize,
#             sumr=sum(revenue))
# df2f$pd <- lubridate::ymd(df2f$pd)
# 
# xyplot(sumr ~ pd | is_follower, data=df2f)
# 
# 
# # q <- '
# # SELECT mem_no,
# # CAST(MONTH(order_date) AS UNSIGNED) month,
# # CAST(YEAR(order_date) AS UNSIGNED) yr,
# # DATE_FORMAT(order_date,"%Y-%m") pd,
# # SUM(amount) rev_krw_sum,
# # AVG(amount) rev_krw_avg,
# # STD(amount) rev_krw_std,
# # (CASE WHEN mem_no IN (SELECT DISTINCT follower_mem_no FROM cconma_memberfollow) THEN 1 ELSE 0 END) is_follower,
# # (CASE WHEN mem_no IN (SELECT DISTINCT following_mem_no FROM cconma_memberfollow) THEN 1 ELSE 0 END) is_followed,
# # (CASE WHEN (mem_no IN (SELECT DISTINCT following_mem_no FROM cconma_memberfollow) AND  mem_no IN (SELECT DISTINCT follower_mem_no FROM cconma_memberfollow) ) THEN 1 ELSE 0 END) is_both,
# # (CASE WHEN (mem_no IN (SELECT DISTINCT following_mem_no FROM cconma_memberfollow) OR  mem_no IN (SELECT DISTINCT follower_mem_no FROM cconma_memberfollow) ) THEN 1 ELSE 0 END) is_either
# # FROM cconma_order
# # GROUP BY pd,is_followed
# # ORDER BY pd asc;
# # '

#---------------MEMBER---------------------------
q <- '
SELECT cm.mem_no,
COUNT(o.mem_no) as order_cnt,
SUM(o.amount) as order_sum,
cm.member_birth as birth,
cm.member_sex as sex,
cm.gender,
cm.member_age as age,
cm.birthday as birthday,
cm.f_marriage as married,
cm.recommender,
DATE(cm.reg_date) reg_date,
DATE(cm.leave_date) leave_date,
cm.status
FROM cconma_member cm
INNER JOIN cconma_order o ON o.mem_no=cm.mem_no
GROUP BY mem_no
HAVING order_cnt > 0
ORDER BY order_cnt desc;
'
con <- dbConnect(RMySQL::MySQL(), dbname = "cconma2", user="root", password="", port=3306)
rs <- dbSendQuery(con, q)
mem <- dbFetch(rs, n= -1); dbDisconnect(con)
mem$reg_date <- lubridate::ymd(mem$reg_date)


##---------------- MONTHLY FOLLOWER RELATIONS ----------------------

months <- c("2015-12", "2016-01", "2016-02", "2016-03", 
            "2016-04", "2016-05", "2016-06", "2016-07", "2016-08",
            "2016-09", "2016-10", "2016-11", 
            "2016-12", "2017-01", "2017-02", "2017-03", "2017-04",
            "2017-05", "2017-06", "2017-07", "2017-08", "2017-09")
fl.cnt <- data.frame( pd=months,
                      follower_cnt=rep(NA,length(months)),
                      followed_cnt=rep(NA,length(months)),
                      unique_mem_cnt=rep(NA,length(months)),
                      relations_cnt=rep(NA,length(months)), stringsAsFactors = F)
fl.lst <- list()

for(i in 1:length(months)) {
  pd <- months[i]
  cat(sprintf('\nquerying followers for pd: %s\n',pd ))
  tmp <- as.numeric(str_split(pd, '-')[[1]])
  y <- tmp[1]
  m <- tmp[2]
  d <- getMonthDays(m)
  follower <- fetch(sprintf('
    SELECT follower_mem_no, COUNT(follower_mem_no) follower_cnt
    FROM cconma_memberfollow
    WHERE DATE(reg_date) <= DATE("%d-%d-%d")
    GROUP BY follower_mem_no; ',y,m,d))
  followed <- fetch(sprintf('
    SELECT following_mem_no followed_mem_no, COUNT(following_mem_no) followed_cnt
    FROM cconma_memberfollow
    WHERE DATE(reg_date) <= DATE("%d-%d-%d")
    GROUP BY followed_mem_no; ',y,m,d))
  total <- fetch(sprintf('
    SELECT COUNT(DISTINCT id) cnt
    FROM cconma_memberfollow
    WHERE DATE(reg_date) <= DATE("%d-%d-%d"); ',y,m,d))
  fl.lst[[pd]] <- list(follower=follower, followed=followed)
  fl.cnt$follower_cnt[fl.cnt$pd == pd] <- length(unique(follower$follower_mem_no))
  fl.cnt$followed_cnt[fl.cnt$pd == pd] <- length(unique(followed$followed_mem_no))
  fl.cnt$unique_mem_cnt[fl.cnt$pd == pd] <- length(unique(c(follower$follower_mem_no, followed$followed_mem_no)))
  fl.cnt$relations_cnt[fl.cnt$pd == pd] <- total$cnt
}
fl.cnt <- cbind(pd_last_date=sapply(fl.cnt$pd, getYearMonthLastDate), fl.cnt)

# vars <- c("follower_cnt", "followed_cnt", "unique_mem_cnt", "relations_cnt")
# matplot(x= as.numeric(as.factor(fl.cnt$pd)), y= fl.cnt[,-c(1:2)], type='l')
# legend('topleft',lty=1:length(vars),col=1:length(vars), legend=vars)

# melt
fl.cnt.w <- melt(fl.cnt[,-2], id.vars = 'pd_last_date', variable.name = 'group')
fl.cnt.w$pd_last_date <- zoo::as.Date(fl.cnt.w$pd_last_date)

ggplot(aes(x=pd_last_date, y=value, colour=group), data=fl.cnt.w) + 
  geom_line() + geom_point() +
  theme_bw()


#---------------- QUANTITY BY MEM_NO, MONTH ------------------------

qty <- fetch('
  SELECT mem_no,
  CAST(MONTH(order_date) AS UNSIGNED) month,
  CAST(YEAR(order_date) AS UNSIGNED) yr,
  DATE_FORMAT(order_date,"%Y-%m") pd,
  SUM(amount) rev_krw_sum,
  AVG(amount) rev_krw_avg,
  STD(amount) rev_krw_std
  FROM cconma_order co
  GROUP BY pd,mem_no
  ORDER BY pd asc;
')
qty$t <- as.numeric(as.factor(qty$pd))

topmems <- c(512, 54, 1024, 1200) #mem$mem_no[2:6]
data.sub <- qty[qty$mem_no %in% topmems, ]
ggplot(aes(x=t,y=rev_krw_sum, colour=factor(mem_no)), data=data.sub) +
  geom_line()


#----------------------------------------------------


q <- '
SELECT mem_no,
CAST(MONTH(order_date) AS UNSIGNED) month,
CAST(YEAR(order_date) AS UNSIGNED) yr,
DATE_FORMAT(order_date,"%Y-%m") pd,
SUM(amount) rev_krw_sum,
AVG(amount) rev_krw_avg,
STD(amount) rev_krw_std,
FROM cconma_order co
INNER JOIN 
GROUP BY pd,mem_no
ORDER BY pd asc;
'
con <- dbConnect(RMySQL::MySQL(), dbname = "cconma2", user="root", password="", port=3306)
rs <- dbSendQuery(con, q)
dff <- dbFetch(rs, n= -1)
dbDisconnect(con)

dff$t <- as.numeric(as.factor(dff$pd))

dim(dff)
head(dff)


xyplot(rev_krw_avg ~ pd, groups=is_followed, data=dffs, type=c('p','r','g'), auto.key =T )


dffsw <- data.frame(y=dff[dff$is_followed==1,c('rev_krw_avg')],
                    x=dff[dff$is_followed==0,c('rev_krw_avg')],
                    pd=unique(dff$pd),
                    t=1:length(unique(dff$pd)))

pre <- c(1, which(dffsw$pd=='2016-02')-1)
post <- c(which(dffsw$pd=='2016-02'), nrow(dffsw))

impact <- CausalImpact(dffsw[,c('y','x')], pre, post,
                       alpha=0.05, model.args=list(nseasons=12))
plot(impact)
summary(impact)
summary(impact, "report")
impact$summary
#----------------- CAUSAL IMPACT ------------------------------------



pre.period <- as.Date(c("2014-01-01", "2014-03-11"))
post.period <- as.Date(c("2014-03-12", "2014-04-10"))

impact <- CausalImpact(data, pre.period, post.period)
plot(impact)
summary(impact)
summary(impact, "report")
impact$summary