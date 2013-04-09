#############################################################################################
#			SS region.
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
                  rain_gauge = 'surfside' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
ss.dat.new<- fetch(RS)
names(ss.dat.new)

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
ss.dat.new$tide<- as.factor(ss.dat.new$tide)
ss.dat.new$weather<-as.factor(ss.dat.new$weather)
ss.dat.new$station<-as.factor(ss.dat.new$station)

#extract date from DateTime
ss.dat.new$dateT<-ss.dat.new$date
ss.dat.new$date<-as.Date(substr(ss.dat.new$dateT,1, 10))
# add year
ss.dat.new$year<-as.numeric(substring(ss.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

ss.dat.new$Class<-as.factor(1)
ss.dat.new$Class<-ifelse(ss.dat.new$etcoc>=104, 2, ss.dat.new$Class)
ss.dat.new$Class<-ifelse(ss.dat.new$etcoc>=500, 3, ss.dat.new$Class)
ss.dat.new$Class<-ordered(ss.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

ss.dat.new<-merge(ss.dat.new, temp, by="date", all.x=T, all.y=F)
names(ss.dat.new)
ss.dat.new$range<-ss.dat.new$highFt - ss.dat.new$lowFt

ss.dat.new$nos8661070_wind_dir<-as.factor(ss.dat.new$nos8661070_wind_dir)
ss.dat.new$sun2_wind_dir<-as.factor(ss.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(ss.dat.new)
ss.regdat<-ss.dat.new
names(ss.regdat)

#create intervention variable
ss.regdat$intervention<-ifelse(ss.regdat$date<"2004-05-01", "before", "after")
ss.regdat$intervention<-ordered(ss.regdat$intervention, levels=c("before", "after"))


names(ss.regdat)
ss.olddat<-na.exclude(ss.regdat[,c(1:22, 44:49)])
nrow(ss.olddat) #n=1285

ss.nexdat<-na.exclude(ss.regdat[,c(1:34, 44:49)])
#ss.nexdat<-ss.nexdat[ss.nexdat$tide!="4100",]
nrow(ss.nexdat) #n=833

ss.oosdat<-na.exclude(ss.regdat)
nrow(ss.oosdat) # n=231      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length

png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSDataDate.png"
		, bg="transparent")

plot(ss.regdat$date, log10(ss.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("Surfside Bacteria data")

dev.off()


#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


ss.oldreg<-lm(log10(etcoc)~
#	intervention
	+ station
	+ salinity
#	+ tide
	+ rain_summary_24	
	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
#	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ weather
#	+ moon_phase
	+ range
	+ lowFt 
	, data=ss.olddat
	, na.action=na.omit

	)

summary(ss.oldreg)
AIC(ss.oldreg)
#plot(ss.oldreg)

#library(car)
vif(ss.oldreg)

qq.plot(ss.oldreg)

library(MASS)
sresid <- studres(ss.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(ss.olddat)
form<-log10(etcoc)~station+salinity+rain_summary_24+rain_summary_48+weather+range+lowFt
whsize<-nrow(ss.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSOldRegCV.png"
		, bg="transparent")
	cvlm(data=ss.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("SS 'Old' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(ss.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(ss.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSOldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('SS "Old Data" ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("ss.oldreg")
summary(ss.oldreg)
print(paste("AIC = ", AIC(ss.oldreg), sep=""))
print("CVr = 0.730")
sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

ss.nexreg<-lm(log10(etcoc)~
# 	+ intervention
	+ station
	+ salinity
#	+ tide
#	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
#	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ radar_rain_summary_24	
#	+ radar_rain_summary_48	
#	+ radar_rain_summary_96
#	+ radar_rain_summary_120
#	+ radar_rain_summary_144
#	+ radar_rain_summary_168
#	+ radar_rainfall_intensity_24
#	+ radar_preceding_dry_day_cnt 
	+ weather
#	+ moon_phase
#	+ range
#	+ lowFt 
	, data=ss.nexdat
	, na.action=na.omit

	)

summary(ss.nexreg)
AIC(ss.nexreg)
#plot(ss.nexreg)

#library(car)
vif(ss.nexreg)
cor.test(ss.nexdat$radar_rain_summary_24,ss.nexdat$rain_summary_24)	

#library(MASS)
sresid <- studres(ss.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 


#     Validation               

names(ss.nexdat)
form<-log10(etcoc)~station+salinity+radar_rain_summary_24+weather
whsize<-nrow(ss.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSNexRegCV.png"
		, bg="transparent")

cvlm(data=ss.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,5), whsize=whsize)
	abline(a=0, b=1)
	title("SS 'Nexrad' Regression Model CV Results")

dev.off()
##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(ss.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(ss.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSNexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('SS NEXRAD Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("ss.nexreg")
summary(ss.nexreg)
print(paste("AIC = ", AIC(ss.nexreg), sep=""))
print("CVr = 0.745")
sink()



#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(ss.oosdat)
ss.oosdat$intervention
ss.oosdat$nos8661070_wind_dir<-factor(ss.oosdat$nos8661070_wind_dir)

ss.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
#	 station
#	- 1
	+ salinity
#	+ tide
	+ radar_rain_summary_24	
#	+ radar_preceding_dry_day_cnt 
#	+ weather
#	+ moon_phase
#	+ range
#	+ lowFt 
#	+ sun2_wind_speed
#	+ sun2_wind_dir
	+ sun2_water_temp
#	+ sun2_salinity
	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
	+ nos8661070_water_temp
#	+ nos8661070_water_level
	, data=ss.oosdat
	, na.action=na.omit

	)

summary(ss.oosreg)
AIC(ss.oosreg)
#plot(ss.oosreg)

#library(car)
vif(ss.oosreg)

cor.test(ss.oosdat$nos8661070_wind_spd, ss.oosdat$sun2_wind_speed)
cor.test(ss.oosdat$salinity, ss.oosdat$sun2_salinity)

qq.plot(ss.oosreg)

#library(MASS)
sresid <- studres(ss.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(ss.oosdat)

ss.oosreg$call
form<-log10(etcoc) ~ +salinity + radar_rain_summary_24 + sun2_water_temp + nos8661070_wind_spd + nos8661070_water_temp
whsize<-nrow(ss.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSIOOSRegCV.png"
		, bg="transparent")

cvlm(data=ss.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("SS 'IOOS' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(ss.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(ss.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSIOOSRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('SS IOOS Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("ss.oosreg")
summary(ss.oosreg)
print(paste("AIC = ", AIC(ss.oosreg), sep=""))
print("CVr = 0.762")
sink()

############################# Develop CART Models

library(rpart)
names(ss.regdat)

boxplot(ss.regdat$radar_rain_summary_24 ~ ss.regdat$Class)
boxplot(ss.regdat$salinity ~ ss.regdat$Class)
boxplot(ss.regdat$sun2_salinity ~ ss.regdat$Class) 

SSTreeOld<-rpart(Class~
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
	, data=ss.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeOld.png"
			, bg="transparent")
		plot(SSTreeOld)
		text(SSTreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("SS With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(SSTreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeOldCP.png")
		plotcp(SSTreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
SSTreeOld.prune<-prune(SSTreeOld, cp=.012)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeOldPrune.png")
	plot(SSTreeOld.prune)
	text(SSTreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(SSTreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(SSTreeOld.prune, ss.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(ss.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: SS, 'Old' data")
	table(pred[,4:5])
sink()


################################### NEXRAD DATA Tree

SSTreeNex<-rpart(Class~
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
	, data=ss.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeNex.png"
			, bg="transparent")
		plot(SSTreeNex)
		text(SSTreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("SS With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(SSTreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeNexCP.png")
		plotcp(SSTreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
SSTreeNex.prune<-prune(SSTreeNex, cp=.021)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeNexPrune.png")
	plot(SSTreeNex.prune)
	text(SSTreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("SS 'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(SSTreeNex.prune)
	

pred<-as.data.frame(predict(SSTreeNex.prune, ss.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(ss.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: SS, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(ss.oosdat)

SSTreeOOS<-rpart(Class~
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
	, data=ss.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeOOS.png"
			, bg="transparent")
		plot(SSTreeOOS)
		text(SSTreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("SS With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(SSTreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeOOSCP.png")
		plotcp(SSTreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info

SSTreeOOS.prune<-prune(SSTreeOOS, cp=.12)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/SSTreeOOSPrune.png")
	plot(SSTreeOOS.prune)
	text(SSTreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("SS 'IOOS' Data Pruned Tree")
	dev.off()
	summary(SSTreeOOS.prune)
	

pred<-as.data.frame(predict(SSTreeOOS.prune, ss.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(ss.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/SS/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: SS, 'IOOS' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(ss.olddat)

ss.rf.dat<-na.exclude(ss.olddat[, c(3, 6:20, 22, 24:28)])
#ss.rf.dat$Class<-ordered(ss.rf.dat$Class, levels = c("Low", "Medium"))
unique(ss.rf.dat$Class)
names(ss.rf.dat)
form<- Class~.
ss.rf <- randomForest(form, data=ss.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(ss.rf), 2)
print(ss.rf)
plot(ss.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(ss.regdat)

dat2<-na.exclude(ss.regdat[,c(3, 6:8, 45:48)])
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


names(ss.nexdat)

ss.nexrf.dat<-na.exclude(ss.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(ss.rf.dat)
form<- Class~.
ss.nexrf <- randomForest(form, data=ss.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(ss.nexrf), 2)
print(ss.nexrf)
plot(ss.nexrf ) #


