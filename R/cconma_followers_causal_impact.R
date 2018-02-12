##
# CCONMA FOLLOWERS RELATIONS DATA EXPLORATION
# SMJ SPECIAL ISSUE  FEB 2018
#
# member; 325900
# +-----------------+---------------------+------+-----+---------+-------+
#   | Field           | Type                | Null | Key | Default | Extra |
#   +-----------------+---------------------+------+-----+---------+-------+
#  | mem_no          | int(10) unsigned    | NO   | PRI | NULL    |       |
#  | name            | varchar(50)         | NO   | MUL |         |       |
#  | member_birth    | varchar(8)          | YES  |     |         |       |
#  | member_sex      | enum('M','F')       | YES  |     | F       |       |
#  | member_age      | tinyint(4)          | YES  |     | NULL    |       |
#  | zip             | varchar(7)          | YES  |     | zipcode |       |
#  | gender          | enum('M','W')       | YES  |     | M       |       |
#  | birthday        | varchar(10)         | YES  |     | NULL    |       |
#  | f_marriage      | enum('Y','N')       | YES  |     | N       |       |
#  | recommender     | int(20)             | YES  |     | 0       |       |
#  | reg_date        | datetime            | NO   | MUL | NULL    |       |
#  | reg_platform    | varchar(16)         | YES  |     | NULL    |       |
#  | last_login_time | datetime            | YES  |     | NULL    |       |
#  | registration_ip | varchar(16)         | YES  |     | NULL    |       |
#  | leave_date      | datetime            | YES  | MUL | NULL    |       |
#  | status          | tinyint(3) unsigned | YES  |     | 1       |       |
#  +-----------------+---------------------+------+-----+---------+-------+
#
# order; 3024021
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
#
# memberfollow; 29191
#  +------------------+-------------+------+-----+---------+----------------+
#  | Field            | Type        | Null | Key | Default | Extra          |
#  +------------------+-------------+------+-----+---------+----------------+
#  | id               | int(11)     | NO   | PRI | NULL    | auto_increment |
#  | following_mem_no | int(10)     | NO   | MUL | 0       |                |
#  | follower_mem_no  | int(10)     | NO   | MUL | 0       |                |
#  | status           | int(11)     | NO   | MUL | 1       |                |
#  | reg_date         | datetime    | YES  | MUL | NULL    |                |
#  | update_date      | datetime    | YES  |     | NULL    |                |
#  | ip_address       | varchar(16) | YES  |     | NULL    |                |
#  | app              | varchar(32) | YES  | MUL | NULL    |                |
#  | app_ver          | varchar(10) | YES  | MUL | NULL    |                |
#  +------------------+-------------+------+-----+---------+----------------+
#  
#  
#  member
#  +---------------------+---------------------+
#  | min(reg_date)       | max(reg_date)       |
#  +---------------------+---------------------+
#  | 0000-00-00 00:00:00 | 2017-09-30 22:00:14 |
#  +---------------------+---------------------+
#  memberfollow
#  +---------------------+---------------------+
#  | min(reg_date)       | max(reg_date)       |
#  +---------------------+---------------------+
#  | 2016-01-25 11:52:09 | 2017-10-16 15:03:39 |
#  +---------------------+---------------------+
#  order
#  +---------------------+---------------------+
#  | min(order_date)     | max(order_date)     |
#  +---------------------+---------------------+
#  | 2012-01-01 00:04:49 | 2017-09-30 23:49:26 |
#  +---------------------+---------------------+
## 

setwd("C:/Users/T430/Google Drive/PhD/Dissertation/sna")

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
fetch <- function(query, dbname = "cconma", user="root", password="", port=3306) {
  con <- dbConnect(RMySQL::MySQL(), dbname=dbname, user=user, password=password, port=port)
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

Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

isEmpty <- function(x){
  if (is.null(x)) 
    return(TRUE)
  return(is.na(x) | is.nan(x) | x == "")
}

##_---------------------------------------------

# con <- odbcConnect(dsn = "cconma2", uid = "root", pwd = "")

q <- '
SELECT 
MONTH(order_date) month,
YEAR(order_date) yr,
DATE_FORMAT(order_date,"%Y-%m") pd,
SUM(amount / 1120) revenue_usd,
COUNT(DISTINCT ocode) order_cnt,
(SUM(amount / 1120) / COUNT(DISTINCT ocode)) avg_order_value_usd
FROM cconma_order
GROUP BY pd;
'

dfor <- fetch(q)

dfor$t <- as.numeric(as.factor(dfor$pd))
dim(dfor)
head(dfor)


follow.start <- which(dfor$pd == '2016-02')

ggplot(aes(x=t, y=revenue_usd), data=dfor) + 
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

#----------------------------------MEMBER---------------------------------------------
q <- '
SELECT cm.mem_no,
COUNT(o.mem_no) as order_cnt,
SUM(o.amount) as order_sum,
TIMESTAMPDIFF(MONTH, DATE(cm.reg_date), "2017-09-30") as num_months,
COUNT(o.mem_no) / TIMESTAMPDIFF(MONTH, DATE(cm.reg_date), "2017-09-30") as avg_m_order_cnt,
SUM(o.amount) / TIMESTAMPDIFF(MONTH, DATE(cm.reg_date), "2017-09-30") as avg_m_order_sum,
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
HAVING order_cnt > 0 AND cm.mem_no > 0
ORDER BY order_cnt desc;
'
mem <- fetch(q)
mem$reg_date <- lubridate::ymd(mem$reg_date)
mem$birthdate <- sapply(1:nrow(mem), function(x){
  birth <- mem$birth[x];  birthday <- mem$birthday[x]
  return(ifelse(!isEmpty(birthday), birthday, sprintf('%s-%s-%s',str_sub(birth,1,4),str_sub(birth,5,6),str_sub(birth,7,8)) ) )
})
mem$birthdate <- ymd(unlist(mem$birthdate))
mem$age <- as.numeric((ymd('2017-09-30') - mem$birthdate) / 365.25)
## fix ymd incorrectly assigned century prefix 20-- instead of 19--
mem$birthdate[which(mem$age < 0)] <- paste0('19',str_sub(mem$birthdate[which(mem$age < 0)], 3,length(mem$birthdate[which(mem$age < 0)])))
mem$age <- as.numeric((ymd('2017-09-30') - mem$birthdate) / 365.25)

saveRDS(mem, file="cconma_mem_df.rds")
##--------------------------------------------------------------------------------------------

## READ members data frame
mem <- readRDS('cconma_mem_df.rds')
##


### view distributions
vars <- c('order_cnt','order_sum','num_months','avg_m_order_cnt','avg_m_order_sum','age')
par(mfrow=c(2,3))
for (i in 1:length(vars)) {
  if (vars[i] %in% c('age','num_months')) {
    x <- mem[,vars[i]]
  } else {
    x <- log(mem[,vars[i]])
  }
  lab <- ifelse(vars[i] %in% c('age','num_months'), vars[i], paste('Ln',vars[i]))
  hist(x, main=sprintf("%s: avg=%.1f, med=%.1f, sd=%.1f,\nmin=%.1f, max=%1.f",
                       lab,mean(x,na.rm = T),median(x,na.rm = T),sd(x,na.rm = T),min(x,na.rm = T),max(x,na.rm = T)))
}

## assign conditional mean age
mem$age[which(is.na(mem$age) & mem$gender=='W')] <- mean(mem$age[which(mem$gender=='W')],na.rm = T)
mem$age[which(is.na(mem$age) & mem$gender=='M')] <- mean(mem$age[which(mem$gender=='M')],na.rm = T)

## save updated mem df
saveRDS(mem, file="cconma_mem_df.rds")

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

saveRDS(mem, file="cconma_mem_df.rds")


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


saveRDS(list(mem=mem,fl.cnt=fl.cnt,fl.lst=fl.lst,qtm=qtm), file="cconma_data_list.rds")

# topmems <- mem$mem_no[c(2,3,4,10000,10001,10002)]
# data.sub <- qtm[qtm$mem_no %in% topmems, ]
# ggplot(aes(x=t,y=rev_krw_sum, colour=factor(mem_no)), data=data.sub) +
#   geom_line()

##------------------CONTROL MEMBERS--------------------------------------
mem.ord <- unique(qtm$mem_no)
# mem.trt <- unique(c(fl.lst$`2017-09`$follower$follower_mem_no, fl.lst$`2017-09`$followed$followed_mem_no))
mem.trt <- unique(c(fl.lst$`2017-09`$follower$follower_mem_no))
mem.ctrl <- mem.ord[which( !(mem.ord %in% mem.trt) )]
cat(sprintf('ordered %s, treated %s, control %s', length(mem.ord),length(mem.trt),length(mem.ctrl)))
## limit to members who ordered and have long enough duration
mem.ctrl <- mem$mem_no[which(mem$mem_no %in% mem.ctrl & mem$reg_date <= '2014-01-25')]
cat(sprintf('ordered %s, treated %s, duration control %s', length(mem.ord),length(mem.trt),length(mem.ctrl)))
df.ctrl <- mem[which(mem$mem_no %in% mem.ctrl),]


### view network adoption by followers & followed
sapply(fl.lst, function(x)sapply(x,nrow))

##------------------TREATMENT := FOLLOWER -------------------------------

## period
pd <- 9
cat(names(fl.lst)[pd])

## build GROUP samples list
g <- list(y=data.frame(),  ## treatment data.frame
          z1=data.frame(), ## control data.frame 1
          z2=data.frame(), ## control data.frame 2
          z=c(),           ## control mem_nos (1,2)
          dropped=c()      ## droped mem_nos
          )

numrow <- nrow(fl.lst[[pd]]$follower)
for(i in 1:numrow) {
  mem_i <- mem[which(mem$mem_no == fl.lst[[pd]]$follower$follower_mem_no[i]), ]
  z.sub <- df.ctrl$mem_no[which( !(df.ctrl$mem_no %in% g$z)
                      & df.ctrl$gender==mem_i$gender
                      & df.ctrl$married==mem_i$married
                      & df.ctrl$age >= (mem_i$age - sd(mem$age,na.rm = T))
                      & df.ctrl$age <= (mem_i$age + sd(mem$age,na.rm = T))
                      & df.ctrl$avg_m_order_sum >= (mem_i$avg_m_order_sum - sd(mem$avg_m_order_sum,na.rm = T))
                      & df.ctrl$avg_m_order_sum <= (mem_i$avg_m_order_sum + sd(mem$avg_m_order_sum,na.rm = T))
                      )]
  if (length(z.sub) < 2) {
    g$dropped = c(g$dropped, mem_i$mem_no)
  } else {
    g$y <- rbind(g$y, mem_i)
    z <- sample(z.sub,size = 2, replace = F)
    g$z <- c(g$z, z)
    g$z1 <- rbind(g$z1, mem[which(mem$mem_no==z[1]),])
    g$z2 <- rbind(g$z2, mem[which(mem$mem_no==z[2]),])
    if (nrow(g$y) != nrow(g$z1) | nrow(g$y) != nrow(g$z2)) stop('mismatch')
  }
  if (i %% 50 == 0) cat(sprintf('%.1f%s\n',100*i/numrow,'%'))
}; cat(sprintf('sample (treat, control): %s',nrow(g$y)))

g$mem_no <- list(y=unique(g$y$mem_no),
                 z1=unique(g$z1$mem_no),
                 z2=unique(g$z2$mem_no))


##------------ RUN CAUSAL IMPACT--------------------------------------


## periods
pds <- unique(qtm$pd)

## CUTOFF DATES
cut.date.start <- as.Date('2014-01-01')  ## '-01-25'
cut.date.treat <- as.Date('2016-01-31')
## GROUPS
# TREATMENT: members who existed a year before and eventually have relations
mem.treat <- g$mem_no$z2
# CONTROL: members who existed a year before and never have a relation (or recommended anyone)
mem.ctrl <- g$mem_no$z1
# mem.ctrl <- sample(mem.ctrl, 10*length(mem.treat), replace = F)
cat(sprintf("\n%s treatment members, %s control members\n", length(mem.treat), length(mem.ctrl)))
## create CausalImpact Data Frame
pds <- pds[ which(pds >= format.Date(cut.date.start, '%Y-%m') ) ]
dfqm <- data.frame(y=rep(NA,length(pds)),x=NA)
rownames(dfqm) <- pds
for (i in 1:length(pds)) {
  pdi <- pds[i]
  last.date <- getYearMonthLastDate(pdi)
  # REVENUE BY GROUP IN PERIOD
  qtm.pd.sub <- qtm[ qtm$pd == pdi , ]
  sums.treat <- qtm.pd.sub$rev_krw_sum[ qtm.pd.sub$mem_no %in% mem.treat ]
  sums.ctrl  <- qtm.pd.sub$rev_krw_sum[ qtm.pd.sub$mem_no %in% mem.ctrl ]
  # COUNTS BY GROUP 
  n.treat <- length(mem.treat)
  n.ctrl <- length(mem.ctrl)
  # CAUSALITY DATA FRAME
  KRW_USD <- 1120
  dfqm$y[i] <- ifelse(n.treat==0, 0,  sums.treat / n.treat ) / KRW_USD
  dfqm$x[i] <- ifelse(n.ctrl==0, 0,  sums.ctrl / n.ctrl )  / KRW_USD
}
## adjust dates format to create pre, post periods
dates <- as.Date(unname(sapply(rownames(dfqm),function(x)getYearMonthLastDate(x))))
pre <- c(dates[1], dates[which(dates==cut.date.treat)-1 ] )
post <- c(dates[ which(dates==cut.date.treat) ], dates[length(dates)])
## format dates
dfqmz <- zoo(cbind(dfqm$y,dfqm$x), dates)
##
## RUN CausalImpact
##
impact <- CausalImpact(dfqmz, pre, post,
                       alpha=0.05, model.args=list(nseasons=12))
plot(impact)
summary(impact)

summary(impact, "report")
impact$summary











##------------ RUN CAUSAL IMPACT--------------------------------------

fl.lst


pds <- unique(qtm$pd)

## Treatment group
## Members Joined within first 2 months
mem.treat <- c()
for (i in 1:length(fl.lst)) {
  er <- fl.lst[[i]]$follower$follower_mem_no
  ed <- fl.lst[[i]]$followed$followed_mem_no
  mem.treat <- unique(c(mem.treat, er, ed))
}

## CUTOFF DATES
cut.date.start <- as.Date('2012-01-01')  ## '-01-25'
cut.date.treat <- as.Date('2015-12-31')
## GROUPS
# TREATMENT: members who existed a year before and eventually have relations
mem.treat <- mem$mem_no[ which(mem$mem_no %in% mem.treat 
                               & mem$reg_date <= cut.date.start 
                               & mem$reg_date != 0
                               ) ]
# CONTROL: members who existed a year before and never have a relation (or recommended anyone)
mem.ctrl <- mem$mem_no[ which( !(mem$mem_no %in% mem.treat) 
                               & mem$reg_date <= cut.date.start 
                               & !(mem$mem_no %in% mem$recommender)
                               & mem$mem_no != 0
                                 ) ]
# mem.ctrl <- sample(mem.ctrl, 10*length(mem.treat), replace = F)

cat(sprintf("\n%s treatment members, %s control members\n", length(mem.treat), length(mem.ctrl)))
## create CausalImpact Data Frame
pds <- pds[ which(pds >= format.Date(cut.date.start, '%Y-%m') ) ]
dfqm <- data.frame(y=rep(NA,length(pds)),x=NA)
rownames(dfqm) <- pds
for (i in 1:length(pds)) {
  pdi <- pds[i]
  last.date <- getYearMonthLastDate(pdi)
  # INDEXES FOR GROUPS
  # treat.idx <- which(!is.na(mem$follower_reg_date) & mem$follower_reg_date < last.date)
  # ctrl.idx <- which( is.na(mem$follower_reg_date) |  mem$follower_reg_date >= last.date)
  # treat.idx <- which(mem$mem_no %in% mem.treat)
  # ctrl.idx <- which( !(mem$mem_no %in% mem.treat) )
  # mem.treat <- mem$mem_no[ treat.idx ]
  # mem.ctrl <- mem$mem_no[ ctrl.idx ]
  #
  # REVENUE BY GROUP IN PERIOD
  qtm.pd.sub <- qtm[ qtm$pd == pdi , ]
  sums.treat <- qtm.pd.sub$rev_krw_sum[ qtm.pd.sub$mem_no %in% mem.treat ]
  sums.ctrl  <- qtm.pd.sub$rev_krw_sum[ qtm.pd.sub$mem_no %in% mem.ctrl ]
  # COUNTS BY GROUP 
  # n.treat <- fl.cnt$follower_cnt[ fl.cnt$pd == pdi ]
  # n.all   <- fl.cnt$mem_cnt[ fl.cnt$pd == pdi ]
  # n.treat <- length(treat.idx)
  # n.ctrl  <- n.all - n.treat
  n.treat <- length(mem.treat)
  n.ctrl <- length(mem.ctrl)
  # CAUSALITY DATA FRAME
  KRW_USD <- 1120
  dfqm$y[i] <- ifelse(n.treat==0, 0,  sums.treat / n.treat ) / KRW_USD
  dfqm$x[i] <- ifelse(n.ctrl==0, 0,  sums.ctrl / n.ctrl )  / KRW_USD
}


dates <- as.Date(unname(sapply(rownames(dfqm),function(x)getYearMonthLastDate(x))))
pre <- c(dates[1], dates[which(dates==cut.date.treat)-1 ] )
post <- c(dates[ which(dates==cut.date.treat) ], dates[length(dates)])

dfqmz <- zoo(cbind(dfqm$y,dfqm$x), dates)
impact <- CausalImpact(dfqmz, pre, post,
                       alpha=0.01, model.args=list(nseasons=12))
plot(impact)
summary(impact)
summary(impact, "report")
impact$summary



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