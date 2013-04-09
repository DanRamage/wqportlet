#############################################################################################
#			MB4 region.
#			Script to query database, condition data, and 
#			develop new models with IOOS data
#			
#			2010-08-20
#############################################################################################

################# Select and condition data

library( RSQLite )
#help(SQLite)
#Invoke the SQLite engine
SQLite( 10, 20000, FALSE, FALSE )

#Create the database driver, and connect
DB <- dbDriver("SQLite")
Con <- dbConnect(DB, "C:\\Heath\\Projects\\SCBeaches\\Data\\DHEC\\dhec_new.db" )

#Query database; sort the results ascending.
RS <- dbSendQuery(Con, "SELECT  * FROM station_summary WHERE 
                  rain_gauge = 'mb4' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
mb4.dat.new<- fetch(RS)
names(mb4.dat.new)

#close resultSet 
dbClearResult(RS)

RS2 <- dbSendQuery(Con, "SELECT  * FROM daily_tide_range 
                  ORDER BY date ASC;" )

Range.new<-fetch(RS2) 

#close resultSet 
dbClearResult(RS2)

# Disconnect from Database
dbDisconnect(Con)

#adjust data types where necessary
mb4.dat.new$tide<- as.factor(mb4.dat.new$tide)
mb4.dat.new$weather<-as.factor(mb4.dat.new$weather)
mb4.dat.new$station<-as.factor(mb4.dat.new$station)

#extract date from DateTime
mb4.dat.new$dateT<-mb4.dat.new$date
mb4.dat.new$date<-as.Date(substr(mb4.dat.new$dateT,1, 10))
# add year
mb4.dat.new$year<-as.numeric(substring(mb4.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

mb4.dat.new$Class<-as.factor(1)
mb4.dat.new$Class<-ifelse(mb4.dat.new$etcoc>=104, 2, mb4.dat.new$Class)
mb4.dat.new$Class<-ifelse(mb4.dat.new$etcoc>=500, 3, mb4.dat.new$Class)
mb4.dat.new$Class<-ordered(mb4.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

#add tide range 
Range.new$dateT<-Range.new$date
Range.new$date<-as.Date(substr(Range.new$dateT,1, 10))     

	#select max and min by date
high<-as.data.frame(as.vector(by(Range.new$level, list(Range.new$date), max)))
names(high)<-c("highFt")
low<-as.data.frame(as.vector(by(Range.new$level, list(Range.new$date), min)))
names(low)<-c("lowFt")
dates<-as.data.frame(unique(Range.new$date))
names(dates)<-c("date")
temp<-as.data.frame(c(dates, high, low))

mb4.dat.new<-merge(mb4.dat.new, temp, by="date", all.x=T, all.y=F)
names(mb4.dat.new)
mb4.dat.new$range<-mb4.dat.new$highFt - mb4.dat.new$lowFt

mb4.dat.new$nos8661070_wind_dir<-as.factor(mb4.dat.new$nos8661070_wind_dir)
mb4.dat.new$sun2_wind_dir<-as.factor(mb4.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(mb4.dat.new)
mb4.regdat<-mb4.dat.new
names(mb4.regdat)

#create intervention variable
mb4.regdat$intervention<-ifelse(mb4.regdat$date<"2004-05-01", "before", "after")
mb4.regdat$intervention<-ordered(mb4.regdat$intervention, levels=c("before", "after"))


names(mb4.regdat)
mb4.olddat<-na.exclude(mb4.regdat[,c(1:22, 44:49)])
nrow(mb4.olddat) #n=633

mb4.nexdat<-na.exclude(mb4.regdat[,c(1:34, 44:49)])
#mb4.nexdat<-mb4.nexdat[mb4.nexdat$tide!="4100",]
nrow(mb4.nexdat) #n=383

mb4.oosdat<-na.exclude(mb4.regdat)
nrow(mb4.oosdat) # n=97      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4DataDate.png"
		, bg="transparent")

plot(mb4.regdat$date, log10(mb4.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("MB4 Bacteria data")

dev.off()


#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


mb4.oldreg<-lm(log10(etcoc)~
	intervention
	+ station
	+ salinity
#	+ tide
	+ rain_summary_24	
#	+ rain_summary_48	
	+ rain_summary_96
	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
	, data=mb4.olddat
	, na.action=na.omit

	)

summary(mb4.oldreg)
AIC(mb4.oldreg)
#plot(mb4.oldreg)

#library(car)
vif(mb4.oldreg)

qq.plot(mb4.oldreg)

library(MASS)
sresid <- studres(mb4.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb4.olddat)
form<-log10(etcoc)~intervention+station+salinity+rain_summary_24+rain_summary_96+rain_summary_120+rainfall_intensity_24+weather+range
whsize<-nrow(mb4.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4OldRegCV.png"
		, bg="transparent")
	cvlm(data=mb4.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("MB4 'Old' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb4.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb4.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4OldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB4 "Old Data" ROC Curve')
dev.off()




sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb4.oldreg")
summary(mb4.oldreg)
print(paste("AIC = ", AIC(mb4.oldreg), sep=""))
print("CVr = 0.620")
sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

mb4.nexreg<-lm(log10(etcoc)~
 	+ intervention
	+ station
	+ salinity
	+ tide
#	+ rain_summary_24	
	+ rain_summary_48	
#	+ rain_summary_96
	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
#	+ rainfall_intensity_24
	+ preceding_dry_day_count 
	+ radar_rain_summary_24	
#	+ radar_rain_summary_48	
#	+ radar_rain_summary_96
#	+ radar_rain_summary_120
#	+ radar_rain_summary_144
	+ radar_rain_summary_168
#	+ radar_rainfall_intensity_24
#	+ radar_preceding_dry_day_cnt 
	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
	, data=mb4.nexdat
	, na.action=na.omit

	)

summary(mb4.nexreg)
AIC(mb4.nexreg)
#plot(mb4.nexreg)

#library(car)
vif(mb4.nexreg)


#library(MASS)
sresid <- studres(mb4.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb4.nexdat)
form<-log10(etcoc)~intervention+station+salinity+tide+rain_summary_48+rain_summary_120+preceding_dry_day_count+radar_rain_summary_24+radar_rain_summary_168+weather+range
whsize<-nrow(mb4.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4NexRegCV.png"
		, bg="transparent")

cvlm(data=mb4.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,54), whsize=whsize)
	abline(a=0, b=1)
	title("MB4 'Nexrad' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb4.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb4.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4NexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB4 NEXRAD Data ROC Curve')
dev.off()

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb4.nexreg")
summary(mb4.nexreg)
print(paste("AIC = ", AIC(mb4.nexreg), sep=""))
print("CVr = 0.703")
sink()



#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(mb4.oosdat)
mb4.oosdat$intervention
mb4.oosdat$nos8661070_wind_dir<-factor(mb4.oosdat$nos8661070_wind_dir)

mb4.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
	 station
#	- 1
	+ salinity
#	+ tide
#	+ radar_rain_summary_24	
	+ radar_preceding_dry_day_cnt 
#	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
#	+ sun2_wind_speed
#	+ sun2_wind_dir
	+ sun2_water_temp
#	+ sun2_salinity
	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
#	+ nos8661070_water_temp
	+ nos8661070_water_level
	, data=mb4.oosdat
	, na.action=na.omit

	)

summary(mb4.oosreg)
AIC(mb4.oosreg)
#plot(mb4.oosreg)

#library(car)
vif(mb4.oosreg)

cor.test(mb4.oosdat$nos8661070_wind_spd, mb4.oosdat$sun2_wind_speed)
cor.test(mb4.oosdat$salinity, mb4.oosdat$sun2_salinity)

qq.plot(mb4.oosreg)

#library(MASS)
sresid <- studres(mb4.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb4.oosdat)

mb4.oosreg$call
form<-log10(etcoc) ~ station+salinity+radar_preceding_dry_day_cnt+range+sun2_water_temp+nos8661070_wind_spd+nos8661070_water_level
whsize<-nrow(mb4.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4IOOSRegCV.png"
		, bg="transparent")

cvlm(data=mb4.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("MB4 'IOOS' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb4.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb4.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4OOSRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB4 IOOS Data ROC Curve')
dev.off()

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb4.oosreg")
summary(mb4.oosreg)
print(paste("AIC = ", AIC(mb4.oosreg), sep=""))
print("CVr = 0.755")
sink()

############################# Develop CART Models

library(rpart)
names(mb4.regdat)

boxplot(mb4.regdat$radar_rain_summary_24 ~ mb4.regdat$Class)
boxplot(mb4.regdat$salinity ~ mb4.regdat$Class)
boxplot(mb4.regdat$sun2_salinity ~ mb4.regdat$Class) 

MB4TreeOld<-rpart(Class~
	station
	+ intervention
	+ salinity
	+ tide
	+ rain_summary_24	
	+ rain_summary_48	
	+ rain_summary_96
	+ rain_summary_120
	+ rain_summary_144
	+ rain_summary_168
	+ rainfall_intensity_24
	+ preceding_dry_day_count 
	+ weather
	+ moon_phase
	+ range
	+ lowFt 
	, data=mb4.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeOld.png"
			, bg="transparent")
		plot(MB4TreeOld)
		text(MB4TreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB4 With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB4TreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeOldCP.png")
		plotcp(MB4TreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB4TreeOld.prune<-prune(MB4TreeOld, cp=.069)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeOldPrune.png")
	plot(MB4TreeOld.prune)
	text(MB4TreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(MB4TreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(MB4TreeOld.prune, mb4.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb4.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB4, 'Old' data")
	table(pred[,4:5])
sink()


################################### NEXRAD DATA Tree

MB4TreeNex<-rpart(Class~
	station
	+ intervention
	+ salinity
	+ tide
#	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
#	+ rainfall_intensity_24
#	+ preceding_dry_day_count
	+ radar_rain_summary_24	
	+ radar_rain_summary_48	
	+ radar_rain_summary_96
	+ radar_rain_summary_120
	+ radar_rain_summary_144
	+ radar_rain_summary_168
	+ radar_rainfall_intensity_24
	+ radar_preceding_dry_day_cnt 
	+ weather
	+ moon_phase
	+ range
	+ lowFt 
	, data=mb4.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeNex.png"
			, bg="transparent")
		plot(MB4TreeNex)
		text(MB4TreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB4 With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB4TreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeNexCP.png")
		plotcp(MB4TreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB4TreeNex.prune<-prune(MB4TreeNex, cp=.049)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeNexPrune.png")
	plot(MB4TreeNex.prune)
	text(MB4TreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("MB4 'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(MB4TreeNex.prune)
	

pred<-as.data.frame(predict(MB4TreeNex.prune, mb4.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb4.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB4, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(mb4.oosdat)

MB4TreeOOS<-rpart(Class~
	station
	+ tide
	+ radar_rain_summary_24	
	+ radar_rain_summary_48	
	+ radar_rain_summary_96
	+ radar_rain_summary_120
	+ radar_rain_summary_144
	+ radar_rain_summary_168
	+ radar_rainfall_intensity_24
	+ radar_preceding_dry_day_cnt
	+ sun2_wind_speed 
	+ sun2_wind_dir
	+ sun2_water_temp
	+ sun2_salinity
	+ nos8661070_wind_spd
	+ nos8661070_wind_dir
	+ nos8661070_water_temp
	+ nos8661070_water_level
	+ moon_phase
	+ range
	+ lowFt 	
	+ highFt
	, data=mb4.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeOOS.png"
			, bg="transparent")
		plot(MB4TreeOOS)
		text(MB4TreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB4 With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB4TreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeOOSCP.png")
		plotcp(MB4TreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info

#########    NA for this region/series

MB4TreeOOS.prune<-MB4TreeOOS
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/MB4TreeOOSPrune.png")
	plot(MB4TreeOOS.prune)
	text(MB4TreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("MB4 'IOOS' Data Pruned Tree")
	dev.off()
	summary(MB4TreeOOS.prune)
	

pred<-as.data.frame(predict(MB4TreeOOS.prune, mb4.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb4.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB4/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB4, 'NEXRAD' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(mb4.olddat)

mb4.rf.dat<-na.exclude(mb4.olddat[, c(3, 6:20, 22, 24:28)])
#mb4.rf.dat$Class<-ordered(mb4.rf.dat$Class, levels = c("Low", "Medium"))
unique(mb4.rf.dat$Class)
names(mb4.rf.dat)
form<- Class~.
mb4.rf <- randomForest(form, data=mb4.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb4.rf), 2)
print(mb4.rf)
plot(mb4.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(mb4.regdat)

dat2<-na.exclude(mb4.regdat[,c(3, 6:8, 45:48)])
dat2.tree <- rpart(Class~
	  station
	+ salinity
	+ moon_phase
	+ tide
	+ range
	+ highFt
	+ lowFt
	+ range
	, data=dat2
	)

plot(dat2.tree)
text(dat2.tree, cex=.75, pretty= '3', use.n=T, xpd=T)

summary(dat2.tree)

round(importance(dat2.rf), 2)
print(dat2.rf)
plot(dat2.rf ) 


# randomForest for the NExrad additions


names(mb4.nexdat)

mb4.nexrf.dat<-na.exclude(mb4.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(mb4.rf.dat)
form<- Class~.
mb4.nexrf <- randomForest(form, data=mb4.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb4.nexrf), 2)
print(mb4.nexrf)
plot(mb4.nexrf ) #


