
#加载相关R包
library(RODBC)
library(data.table)
library(zoo)#日期处理
library(car)#分型重编码

#从设置的数据源中选择待处理的表，从表中选择相关字段，对进行合并处理
conn <- odbcConnect("",uid="",pwd="")
cd <- sqlQuery(conn, 'select *  from BA_CDFX_BRJLK',as.is=TRUE)
odbcClose(conn)

#转换变量属性
cd$zfy = as.numeric(cd$zfy)
cd$xyf = as.numeric(cd$xyf)
cd$chengyf = as.numeric(cd$chengyf)
cd$caoyf = as.numeric(cd$caoyf)
cd$qtf1 = as.numeric(cd$qtf1)
cd$qtf2 = as.numeric(cd$qtf2)


#利用data.table软件包进行数据处理
cd <- data.table(cd)

#变量合并
cd[, ypf :=xyf + chengyf + caoyf]
cd[, qtf :=qtf1 + qtf2]
cd[, c("xyf","chengyf","caoyf","qtf1","qtf2") := NULL]

#日期处理
cd[, year := as.Date(ISOdate(year(as.POSIXlt(cyrq)),1,1))]

#科室ks_key
cd$ks_key = cd$cyks
cd[, cyks := NULL]

# 将C、D重新编码为CD 
cd$cd_key <- recode(cd$cd,"c('C','D')='CD'")
cd[, cd := NULL]

#科室年度住院天数、总费用、药费、其他费等的最大值，最小值等计算
cd.stats <- cd[, rbindlist(lapply(names(.SD), 
                                  function(zbmx_id){  
                                    return(list(zbmx_id=zbmx_id,
                                                year = year,
                                                ks_key = ks_key,
                                                max_val=max(.SD[[zbmx_id]]),
                                                min_val=min(.SD[[zbmx_id]]),
                                                mid_val=quantile(zyts, .50, na.rm<-TRUE),
                                                avg_val=mean(.SD[[zbmx_id]]),
                                                std_val=sd(.SD[[zbmx_id]]),
                                                q3_val=quantile(.SD[[zbmx_id]], .75, na.rm<-TRUE),
                                                q1_val=quantile(.SD[[zbmx_id]], .25, na.rm<-TRUE),
                                                gxrq='',
                                                val=''
                                    ))
                                  })),
               by = c('ks_key','year'),
               .SDcols=c("zyts","zfy","ypf","qtf") ]
cd.stats <- cd.stats[, list(year = year,
                            ks_key = ks_key,
                            zbmx_id=zbmx_id,
                            year = year,
                            ks_key = ks_key,
                            cd_key='ABCD',
                            max_val=max_val,
                            min_val=min_val,
                            mid_val=mid_val,
                            avg_val=avg_val,
                            std_val=std_val,
                            q3_val=q3_val,
                            q1_val=q1_val,
                            gxrq=gxrq,
                            val=val )]


#科室各病例分型的汇总情况
cd.stats_1 <- cd[, rbindlist(lapply(names(.SD), 
                                    function(zbmx_id){  
                                      return(list(zbmx_id=zbmx_id,
                                                  year = year,
                                                  ks_key = ks_key,
                                                  cd_key = cd_key,
                                                  max_val=max(.SD[[zbmx_id]]),
                                                  min_val=min(.SD[[zbmx_id]]),
                                                  mid_val=quantile(zyts, .50, na.rm<-TRUE),
                                                  avg_val=mean(.SD[[zbmx_id]]),
                                                  std_val=sd(.SD[[zbmx_id]]),
                                                  q3_val=quantile(.SD[[zbmx_id]], .75, na.rm<-TRUE),
                                                  q1_val=quantile(.SD[[zbmx_id]], .25, na.rm<-TRUE),
                                                  gxrq='',
                                                  val=''
                                      ))
                                    })),
                 by = c('ks_key','cd_key','year'),
                 .SDcols=c("zyts","zfy","ypf","qtf") ]

#合并所有病例(cd_key='ABCD')和各病型的统计量
cd.stats <- as.data.frame(cd.stats)[,c(-1,-2)]
cd.stats_1 <- as.data.frame(cd.stats_1)[,c(-1,-2,-3)]
colnames(cd.stats_1) <- colnames(cd.stats)
# rbind
cd.stats_2 <- rbind(cd.stats,cd.stats_1)

#写入数据库
library(RODBC)
conn=odbcConnect("",uid="",pwd="")
sqlSave(conn, cd.stats_2, "FACT_CD_KS_VALUE_Y",safer=FALSE,append=TRUE,rownames=FALSE)
odbcClose(conn)
