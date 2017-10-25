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
mem <- fetch(q)
mem$reg_date <- lubridate::ymd(mem$reg_date)


## follower relations
follower <- fetch('
                  SELECT follower_mem_no, 
                  MIN(DATE(reg_date)) follower_reg_date
                  FROM cconma_memberfollow
                  GROUP BY follower_mem_no;')
follower$follower_reg_date <- ymd(follower$follower_reg_date)
## following relations
followed <- fetch('
                  SELECT following_mem_no followed_mem_no, 
                  MIN(DATE(reg_date)) followed_reg_date
                  FROM cconma_memberfollow
                  GROUP BY followed_mem_no;')
followed$followed_reg_date <- ymd(followed$followed_reg_date)

mem <- merge(mem, follower, by.x='mem_no', by.y='follower_mem_no', all.x = T)
mem <- merge(mem, followed, by.x='mem_no', by.y='followed_mem_no', all.x = T)

##---------------- MONTHLY FOLLOWER RELATIONS ----------------------

months <- c("2012-01", "2012-02", "2012-03", "2012-04", "2012-05", "2012-06", "2012-07", "2012-08",
            "2012-09", "2012-10", "2012-11", "2012-12", "2013-01", "2013-02", "2013-03", "2013-04",
            "2013-05", "2013-06", "2013-07", "2013-08", "2013-09", "2013-10", "2013-11", "2013-12",
            "2014-01", "2014-02", "2014-03", "2014-04", "2014-05", "2014-06", "2014-07", "2014-08",
            "2014-09", "2014-10", "2014-11", "2014-12", "2015-01", "2015-02", "2015-03", "2015-04",
            "2015-05", "2015-06", "2015-07", "2015-08", "2015-09", "2015-10", "2015-11", "2015-12",
            "2016-01", "2016-02", "2016-03", "2016-04", "2016-05", "2016-06", "2016-07", "2016-08",
            "2016-09", "2016-10", "2016-11", "2016-12", "2017-01", "2017-02", "2017-03", "2017-04",
            "2017-05", "2017-06", "2017-07", "2017-08", "2017-09")
fl.cnt <- data.frame( pd=months,
                      follower_cnt=rep(NA,length(months)),
                      followed_cnt=rep(NA,length(months)),
                      unique_mem_cnt=rep(NA,length(months)),
                      relations_cnt=rep(NA,length(months)), 
                      mem_cnt=rep(NA,length(months)), 
                      stringsAsFactors = F)
fl.lst <- list()

for(i in 1:length(months)) {
  pd <- months[i]
  cat(sprintf('\nquerying followers for pd: %s\n',pd ))
  tmp <- as.numeric(str_split(pd, '-')[[1]])
  y <- tmp[1]
  m <- tmp[2]
  d <- getMonthDays(m)
  follower <- fetch(sprintf('SELECT follower_mem_no, COUNT(follower_mem_no) follower_cnt
                            FROM cconma_memberfollow
                            WHERE DATE(reg_date) <= DATE("%d-%d-%d")
                            GROUP BY follower_mem_no; ',y,m,d))
  followed <- fetch(sprintf('SELECT following_mem_no followed_mem_no, COUNT(following_mem_no) followed_cnt
                            FROM cconma_memberfollow
                            WHERE DATE(reg_date) <= DATE("%d-%d-%d")
                            GROUP BY followed_mem_no; ',y,m,d))
  total <- fetch(sprintf('SELECT COUNT(DISTINCT id) cnt
                         FROM cconma_memberfollow
                         WHERE DATE(reg_date) <= DATE("%d-%d-%d"); ',y,m,d))
  all <- fetch(sprintf('SELECT COUNT(DISTINCT mem_no) cnt
                         FROM cconma_member
                         WHERE DATE(reg_date) <= DATE("%d-%d-%d"); ',y,m,d))
  if (nrow(follower) > 0 | nrow(followed) > 0) {
    fl.lst[[pd]] <- list(follower=follower, followed=followed)
    fl.cnt$follower_cnt[fl.cnt$pd == pd] <- length(unique(follower$follower_mem_no))
    fl.cnt$followed_cnt[fl.cnt$pd == pd] <- length(unique(followed$followed_mem_no))
    fl.cnt$unique_mem_cnt[fl.cnt$pd == pd] <- length(unique(c(follower$follower_mem_no, followed$followed_mem_no)))
    fl.cnt$relations_cnt[fl.cnt$pd == pd] <- total$cnt
  } else {
    fl.cnt$follower_cnt[fl.cnt$pd == pd] <- 0
    fl.cnt$followed_cnt[fl.cnt$pd == pd] <- 0
    fl.cnt$unique_mem_cnt[fl.cnt$pd == pd] <- 0
    fl.cnt$relations_cnt[fl.cnt$pd == pd] <- 0
  }

  fl.cnt$mem_cnt[fl.cnt$pd == pd] <- all$cnt
}
fl.cnt <- cbind(pd_last_date=sapply(fl.cnt$pd, getYearMonthLastDate), fl.cnt)
fl.cnt$pct_unique <-  fl.cnt$unique_mem_cnt / fl.cnt$mem_cnt

# vars <- c("follower_cnt", "followed_cnt", "unique_mem_cnt", "relations_cnt")
# matplot(x= as.numeric(as.factor(fl.cnt$pd)), y= fl.cnt[,-c(1:2)], type='l')
# legend('topleft',lty=1:length(vars),col=1:length(vars), legend=vars)

# melt
fl.cnt.w <- melt(fl.cnt[,-2], id.vars = 'pd_last_date', variable.name = 'group')
fl.cnt.w$pd_last_date <- zoo::as.Date(fl.cnt.w$pd_last_date)

ggplot(aes(x=pd_last_date, y=value, colour=group), data=fl.cnt.w) + 
  geom_line() + geom_point() +
  theme_bw()



# #---------------- QUANTITY BY MONTH ------------------------
# 
# qty <- fetch('
#              SELECT CAST(MONTH(order_date) AS UNSIGNED) month,
#              CAST(YEAR(order_date) AS UNSIGNED) yr,
#              DATE_FORMAT(order_date,"%Y-%m") pd,
#              SUM(amount) rev_krw_sum,
#              AVG(amount) rev_krw_avg,
#              STD(amount) rev_krw_std,
#              MAX(amount) rev_krw_max,
#              MIN(amount) rev_krw_min,
#              COUNT(mem_no) cnt
#              FROM cconma_order co
#              GROUP BY pd
#              ORDER BY pd asc;
#              ')
# qty$t <- as.numeric(as.factor(qty$pd))
# 
# topmems <- mem$mem_no[c(2,3,4,10000,10001,10002)]
# data.sub <- qty[qty$mem_no %in% topmems, ]
# ggplot(aes(x=t,y=rev_krw_sum, colour=factor(mem_no)), data=data.sub) +
#   geom_line()




#---------------- QUANTITY BY MEM_NO, MONTH ------------------------

qtm <- fetch('
             SELECT mem_no,
             CAST(MONTH(order_date) AS UNSIGNED) month,
             CAST(YEAR(order_date) AS UNSIGNED) yr,
             DATE_FORMAT(order_date,"%Y-%m") pd,
             SUM(amount) rev_krw_sum,
             AVG(amount) rev_krw_avg,
             STD(amount) rev_krw_std,
             MAX(amount) rev_krw_max,
             MIN(amount) rev_krw_min,
             COUNT(amount) rev_krw_cnt
             FROM cconma_order co
             GROUP BY pd,mem_no
             ORDER BY pd asc;
             ')
qtm$t <- as.numeric(as.factor(qtm$pd))

topmems <- mem$mem_no[c(2,3,4,10000,10001,10002)]
data.sub <- qtm[qtm$mem_no %in% topmems, ]
ggplot(aes(x=t,y=rev_krw_sum, colour=factor(mem_no)), data=data.sub) +
  geom_line()

pds <- unique(qtm$pd)

dfqm <- data.frame(y=rep(NA,length(pds)),x=NA)
rownames(dfqm) <- pds

for (i in 1:length(pds)) {
  pdi <- pds[i]
  last.date <- getYearMonthLastDate(pdi)
  #
  treat.idx <- which(!is.na(mem$follower_reg_date) & mem$follower_reg_date < last.date)
  ctrl.idx <- which( is.na(mem$follower_reg_date) |  mem$follower_reg_date >= last.date)
  mem.treat <- mem$mem_no[ treat.idx ]
  mem.ctrl <- mem$mem_no[ ctrl.idx ]
  #
  qtm.pd.sub <- qtm[ qtm$pd == pdi , ]
  sums.treat <- qtm.pd.sub$rev_krw_sum[ qtm.pd.sub$mem_no %in% mem.treat ]
  sums.ctrl  <- qtm.pd.sub$rev_krw_sum[ qtm.pd.sub$mem_no %in% mem.ctrl ]
  #
  n.treat <- fl.cnt$follower_cnt[ fl.cnt$pd == pdi ]
  n.ctrl <- fl.cnt$follower_cnt[ fl.cnt$pd == pdi ]
  n.all <- fl.cnt$mem_cnt[ fl.cnt$pd == pdi ]
  #
  dfqm$y[i] <- 
  dfqm$x[i] <- 
}


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