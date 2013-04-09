#############################################################################################
#			MB2 region.
#			Script to query database, condition data, and 
#			develop new models with IOOS data
#			
#			2010-08-19
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
                  rain_gauge = 'mb2' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
mb2.dat.new<- fetch(RS)
names(mb2.dat.new)

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
mb2.dat.new$tide<- as.factor(mb2.dat.new$tide)
mb2.dat.new$weather<-as.factor(mb2.dat.new$weather)
mb2.dat.new$station<-as.factor(mb2.dat.new$station)

#extract date from DateTime
mb2.dat.new$dateT<-mb2.dat.new$date
mb2.dat.new$date<-as.Date(substr(mb2.dat.new$dateT,1, 10))
# add year
mb2.dat.new$year<-as.numeric(substring(mb2.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

mb2.dat.new$Class<-as.factor(1)
mb2.dat.new$Class<-ifelse(mb2.dat.new$etcoc>=104, 2, mb2.dat.new$Class)
mb2.dat.new$Class<-ifelse(mb2.dat.new$etcoc>=500, 3, mb2.dat.new$Class)
mb2.dat.new$Class<-ordered(mb2.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

mb2.dat.new<-merge(mb2.dat.new, temp, by="date", all.x=T, all.y=F)
names(mb2.dat.new)
mb2.dat.new$range<-mb2.dat.new$highFt - mb2.dat.new$lowFt

mb2.dat.new$nos8661070_wind_dir<-as.factor(mb2.dat.new$nos8661070_wind_dir)
mb2.dat.new$sun2_wind_dir<-as.factor(mb2.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(mb2.dat.new)
mb2.regdat<-mb2.dat.new
names(mb2.regdat)

#create intervention variable
mb2.regdat$intervention<-ifelse(mb2.regdat$date<"2004-05-01", "before", "after")
mb2.regdat$intervention<-ordered(mb2.regdat$intervention, levels=c("before", "after"))


names(mb2.regdat)
mb2.olddat<-na.exclude(mb2.regdat[,c(1:22, 44:49)])
nrow(mb2.olddat) #n=863

mb2.nexdat<-na.exclude(mb2.regdat[,c(1:34, 44:49)])
#mb2.nexdat<-mb2.nexdat[mb2.nexdat$tide!="4100",]
nrow(mb2.nexdat) #n=473

mb2.oosdat<-na.exclude(mb2.regdat)
nrow(mb2.oosdat) # n=97      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2DataDate.png"
		, bg="transparent")

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2DataDate.png"
		, bg="transparent")

plot(mb2.regdat$date, log10(mb2.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("MB2 Bacteria data")

dev.off()

#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


mb2.oldreg<-lm(log10(etcoc)~
#	intervention
	+ station
	+ salinity
	+ tide
	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
	, data=mb2.olddat
	, na.action=na.omit

	)

summary(mb2.oldreg)
AIC(mb2.oldreg)
#plot(mb2.oldreg)

#library(car)
vif(mb2.oldreg)

qq.plot(mb2.oldreg)

library(MASS)
sresid <- studres(mb2.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb2.olddat)
form<-log10(etcoc)~station+salinity+tide+rain_summary_24+rain_summary_120+rainfall_intensity_24+weather+range
whsize<-nrow(mb2.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2OldRegCV.png"
		, bg="transparent")
	cvlm(data=mb2.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("MB2 'Old' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb2.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb2.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2OldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB2 "Old Data" ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb2.oldreg")
summary(mb2.oldreg)
print(paste("AIC = ", AIC(mb2.oldreg), sep=""))
print("CVr = 0.680")
sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

mb2.nexreg<-lm(log10(etcoc)~
# 	+ intervention
	+ station
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
	, data=mb2.nexdat
	, na.action=na.omit

	)

summary(mb2.nexreg)
AIC(mb2.nexreg)
#plot(mb2.nexreg)

#library(car)
vif(mb2.nexreg)

#library(MASS)
sresid <- studres(mb2.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb2.nexdat)
form<-log10(etcoc)~station+salinity+tide+radar_rain_summary_24+weather
whsize<-nrow(mb2.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2NexRegCV.png"
		, bg="transparent")

cvlm(data=mb2.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,5), whsize=whsize)
	abline(a=0, b=1)
	title("MB2 'Nexrad' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb2.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb2.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2NexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB2 NEXRAD Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb2.nexreg")
summary(mb2.nexreg)
print(paste("AIC = ", AIC(mb2.nexreg), sep=""))
print"CVr = 0.682"
sink()



#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(mb2.oosdat)
mb2.oosdat$intervention
mb2.oosdat$nos8661070_wind_dir<-factor(mb2.oosdat$nos8661070_wind_dir)

mb2.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
#	 station
#	- 1
	+ salinity
#	+ tide
	+ radar_rain_summary_24	
	+ radar_preceding_dry_day_cnt 
#	+ weather
#	+ moon_phase
#	+ range
#	+ lowFt 
#	+ sun2_wind_speed
#	+ sun2_wind_dir
#	+ sun2_water_temp
#	+ sun2_salinity
#	+ nos8661070_wind_spd
	+ nos8661070_wind_dir	
#	+ nos8661070_water_temp
#	+ nos8661070_water_level
	, data=mb2.oosdat
	, na.action=na.omit

	)

summary(mb2.oosreg)
AIC(mb2.oosreg)
#plot(mb2.oosreg)

#library(car)
vif(mb2.oosreg)

cor.test(mb2.oosdat$nos8661070_wind_spd, mb2.oosdat$sun2_wind_speed)
cor.test(mb2.oosdat$salinity, mb2.oosdat$sun2_salinity)

qq.plot(mb2.oosreg)

library(MASS)
sresid <- studres(mb2.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb2.oosdat)

mb2.oosreg$call
form<-log10(etcoc) ~ salinity + radar_rain_summary_24 + radar_preceding_dry_day_cnt+ nos8661070_wind_dir 
whsize<-nrow(mb2.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2IOOSRegCV.png"
		, bg="transparent")

cvlm(data=mb2.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("MB2 'IOOS' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb2.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb2.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2IOOSRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB2 IOOS Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb2.oosreg")
summary(mb2.oosreg)
print(paste("AIC = ",AIC(mb2.oosreg), sep=""))
print("CVr = 0.638")
sink()

############################# Develop CART Models

library(rpart)
names(mb2.regdat)

boxplot(mb2.regdat$radar_rain_summary_24 ~ mb2.regdat$Class)
boxplot(mb2.regdat$salinity ~ mb2.regdat$Class)
boxplot(mb2.regdat$sun2_salinity ~ mb2.regdat$Class) 

MB2TreeOld<-rpart(Class~
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
	, data=mb2.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeOld.png"
			, bg="transparent")
		plot(MB2TreeOld)
		text(MB2TreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB2 With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB2TreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeOldCP.png")
		plotcp(MB2TreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB2TreeOld.prune<-prune(MB2TreeOld, cp=.02)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeOldPrune.png")
	plot(MB2TreeOld.prune)
	text(MB2TreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(MB2TreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(MB2TreeOld.prune, mb2.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb2.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB2, 'Old' data")
	table(pred[,4:5])
sink()


################################### NEXRAD DATA Tree

MB2TreeNex<-rpart(Class~
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
	, data=mb2.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeNex.png"
			, bg="transparent")
		plot(MB2TreeNex)
		text(MB2TreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB2 With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB2TreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeNexCP.png")
		plotcp(MB2TreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB2TreeNex.prune<-prune(MB2TreeNex, cp=.072)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeNexPrune.png")
	plot(MB2TreeNex.prune)
	text(MB2TreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("MB2 'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(MB2TreeNex.prune)
	

pred<-as.data.frame(predict(MB2TreeNex.prune, mb2.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb2.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB2, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(mb2.oosdat)

MB2TreeOOS<-rpart(Class~
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
	, data=mb2.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeOOS.png"
			, bg="transparent")
		plot(MB2TreeOOS)
		text(MB2TreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB2 With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB2TreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeOOSCP.png")
		plotcp(MB2TreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info

#########    NA for this region/series

#MB2TreeOOS.prune<-prune(MB2TreeOOS, cp=.025)
#	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/MB2TreeOOSPrune.png")
#	plot(MB2TreeOOS.prune)
#	text(MB2TreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
#	title("MB2 'IOOS' Data Pruned Tree")
#	dev.off()
#	summary(MB2TreeOOS.prune)
	
# FOR THIS REGION ONLY
# MB2TreeOOS.prune<-MB2TreeOOS

pred<-as.data.frame(predict(MB2TreeOOS.prune, mb2.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb2.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB2, 'NEXRAD' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(mb2.olddat)

mb2.rf.dat<-na.exclude(mb2.olddat[, c(3, 6:20, 22, 24:28)])
#mb2.rf.dat$Class<-ordered(mb2.rf.dat$Class, levels = c("Low", "Medium"))
unique(mb2.rf.dat$Class)
names(mb2.rf.dat)
form<- Class~.
mb2.rf <- randomForest(form, data=mb2.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb2.rf), 2)
print(mb2.rf)
plot(mb2.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(mb2.regdat)

dat2<-na.exclude(mb2.regdat[,c(3, 6:8, 45:48)])
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


names(mb2.nexdat)

mb2.nexrf.dat<-na.exclude(mb2.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(mb2.rf.dat)
form<- Class~.
mb2.nexrf <- randomForest(form, data=mb2.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb2.nexrf), 2)
print(mb2.nexrf)
plot(mb2.nexrf ) #


