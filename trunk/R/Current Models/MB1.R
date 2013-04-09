#############################################################################################
#			MB1 region.
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
                  rain_gauge = 'mb1' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
mb1.dat.new<- fetch(RS)
names(mb1.dat.new)

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
mb1.dat.new$tide<- as.factor(mb1.dat.new$tide)
mb1.dat.new$weather<-as.factor(mb1.dat.new$weather)
mb1.dat.new$station<-as.factor(mb1.dat.new$station)

#extract date from DateTime
mb1.dat.new$dateT<-mb1.dat.new$date
mb1.dat.new$date<-as.Date(substr(mb1.dat.new$dateT,1, 10))
# add year
mb1.dat.new$year<-as.numeric(substring(mb1.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

mb1.dat.new$Class<-as.factor(1)
mb1.dat.new$Class<-ifelse(mb1.dat.new$etcoc>=104, 2, mb1.dat.new$Class)
mb1.dat.new$Class<-ifelse(mb1.dat.new$etcoc>=500, 3, mb1.dat.new$Class)
mb1.dat.new$Class<-ordered(mb1.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

mb1.dat.new<-merge(mb1.dat.new, temp, by="date", all.x=T, all.y=F)
names(mb1.dat.new)
mb1.dat.new$range<-mb1.dat.new$highFt - mb1.dat.new$lowFt

mb1.dat.new$nos8661070_wind_dir<-as.factor(mb1.dat.new$nos8661070_wind_dir)
mb1.dat.new$sun2_wind_dir<-as.factor(mb1.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(mb1.dat.new)
mb1.regdat<-mb1.dat.new
names(mb1.regdat)

#create intervention variable
mb1.regdat$intervention<-ifelse(mb1.regdat$date<"2004-05-01", "before", "after")
mb1.regdat$intervention<-ordered(mb1.regdat$intervention, levels=c("before", "after"))


names(mb1.regdat)
mb1.olddat<-na.exclude(mb1.regdat[,c(1:22, 44:49)])
nrow(mb1.olddat) #n=763

mb1.nexdat<-na.exclude(mb1.regdat[,c(1:34, 44:49)])
#mb1.nexdat<-mb1.nexdat[mb1.nexdat$tide!="4100",]
nrow(mb1.nexdat) #n=538

mb1.oosdat<-na.exclude(mb1.regdat)
nrow(mb1.oosdat) # n=155      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1DataDate.png"
		, bg="transparent")

plot(mb1.regdat$date, log10(mb1.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("MB1 Bacteria data")
dev.off()


#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


mb1.oldreg<-lm(log10(etcoc)~
#	intervention
	+ station
	+ salinity
	+ tide
	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
#	+ rainfall_intensity_24
	+ preceding_dry_day_count 
	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
	, data=mb1.olddat
	, na.action=na.omit

	)


summary(mb1.oldreg)
AIC(mb1.oldreg)
#plot(mb1.oldreg)


#library(car)
vif(mb1.oldreg)

qq.plot(mb1.oldreg)

library(MASS)
sresid <- studres(mb1.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb1.olddat)
form<-log10(etcoc)~station+salinity+tide+rain_summary_24+preceding_dry_day_count+weather+range
whsize<-nrow(mb1.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1OldRegCV.png"
		, bg="transparent")
	cvlm(data=mb1.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("MB1 'Old' Regression Model CV Results")

dev.off()

#      ROC Curve 
library(ROCR)
labels<-ordered(ifelse(mb1.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb1.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1OldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB1 Old Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb1.oldreg") 
summary(mb1.oldreg)
print("AIC = ") 
AIC(mb1.oldreg)
print("CVr = 0.483")

sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

mb1.nexreg<-lm(log10(etcoc)~
 	+ intervention
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
	, data=mb1.nexdat
	, na.action=na.omit

	)

summary(mb1.nexreg)
AIC(mb1.nexreg)
#plot(mb1.nexreg)

#library(car)
vif(mb1.nexreg)

#library(MASS)
sresid <- studres(mb1.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb1.nexdat)
form<-log10(etcoc)~intervention+station+salinity+tide+radar_rain_summary_24+weather
whsize<-nrow(mb1.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1NexRegCV.png"
		, bg="transparent")

cvlm(data=mb1.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,5), whsize=whsize)
	abline(a=0, b=1)
	title("MB1 'Nexrad' Regression Model CV Results")

dev.off()

#      ROC Curve 
library(ROCR)
labels<-ordered(ifelse(mb1.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb1.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1NexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB1 NEXRAD Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)

print("mb1.nexreg") 
summary(mb1.nexreg)
print("AIC = ") 
AIC(mb1.nexreg)
print("CVr = 0.517")

sink()



#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(mb1.oosdat)
mb1.oosdat$intervention
mb1.oosdat$nos8661070_wind_dir<-factor(mb1.oosdat$nos8661070_wind_dir)

mb1.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
	 station
#	- 1
	+ salinity
#	+ tide
	+ radar_rain_summary_24	
#	+ radar_preceding_dry_day_cnt 
	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
	+ sun2_wind_speed
#	+ sun2_wind_dir
#	+ sun2_water_temp
	+ sun2_salinity
#	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
#	+ nos8661070_water_temp
#	+ nos8661070_water_level
	, data=mb1.oosdat
	, na.action=na.omit

	)

summary(mb1.oosreg)
AIC(mb1.oosreg)
#plot(mb1.oosreg)

#library(car)
vif(mb1.oosreg)

cor.test(mb1.oosdat$nos8661070_wind_spd, mb1.oosdat$sun2_wind_speed)
cor.test(mb1.oosdat$salinity, mb1.oosdat$sun2_salinity)

qq.plot(mb1.oosreg)

library(MASS)
sresid <- studres(mb1.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb1.oosdat)

mb1.oosreg$call
form<-log10(etcoc) ~ station + salinity + radar_rain_summary_24 + weather + range + sun2_wind_speed + sun2_salinity 
whsize<-nrow(mb1.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1IOOSRegCV.png"
		, bg="transparent")

cvlm(data=mb1.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("MB1 'IOOS' Regression Model CV Results")

dev.off()

#      ROC Curve 
library(ROCR)
labels<-ordered(ifelse(mb1.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb1.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1IOOSRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB1 IOOS Data ROC Curve')
dev.off()



sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
summary(mb1.oosreg)
AIC(mb1.oosreg)
sink()

############################# Develop CART Models

library(rpart)
names(mb1.regdat)

boxplot(mb1.regdat$radar_rain_summary_24 ~ mb1.regdat$Class)
boxplot(mb1.regdat$salinity ~ mb1.regdat$Class)
boxplot(mb1.regdat$sun2_salinity ~ mb1.regdat$Class) 

MB1TreeOld<-rpart(Class~
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
	, data=mb1.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeOld.png"
			, bg="transparent")
		plot(MB1TreeOld)
		text(MB1TreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB1 With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB1TreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeOldCP.png")
		plotcp(MB1TreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB1TreeOld.prune<-prune(MB1TreeOld, cp=.023)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeOldPrune.png")
	plot(MB1TreeOld.prune)
	text(MB1TreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(MB1TreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(MB1TreeOld.prune, mb1.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb1.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB1, 'Old' data")
	table(pred[,4:5])
sink()

################################### NEXRAD DATA Tree

MB1TreeNex<-rpart(Class~
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
	, data=mb1.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeNex.png"
			, bg="transparent")
		plot(MB1TreeNex)
		text(MB1TreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB1 With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB1TreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeNexCP.png")
		plotcp(MB1TreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB1TreeNex.prune<-prune(MB1TreeNex, cp=.042)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeNexPrune.png")
	plot(MB1TreeNex.prune)
	text(MB1TreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(MB1TreeNex.prune)
	

pred<-as.data.frame(predict(MB1TreeNex.prune, mb1.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb1.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB1, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(mb1.oosdat)

MB1TreeOOS<-rpart(Class~
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
	, data=mb1.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeOOS.png"
			, bg="transparent")
		plot(MB1TreeOOS)
		text(MB1TreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB1 With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB1TreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeOOSCP.png")
		plotcp(MB1TreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB1TreeOOS.prune<-prune(MB1TreeOOS, cp=.025)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/MB1TreeOOSPrune.png")
	plot(MB1TreeOOS.prune)
	text(MB1TreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'IOOS' Data Pruned Tree")
	dev.off()
	summary(MB1TreeOOS.prune)
	

pred<-as.data.frame(predict(MB1TreeOOS.prune, mb1.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb1.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB1/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB1, 'NEXRAD' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(mb1.olddat)

mb1.rf.dat<-na.exclude(mb1.olddat[, c(3, 6:20, 22, 24:28)])
#mb1.rf.dat$Class<-ordered(mb1.rf.dat$Class, levels = c("Low", "Medium"))
unique(mb1.rf.dat$Class)
names(mb1.rf.dat)
form<- Class~.
mb1.rf <- randomForest(form, data=mb1.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb1.rf), 2)
print(mb1.rf)
plot(mb1.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(mb1.regdat)

dat2<-na.exclude(mb1.regdat[,c(3, 6:8, 45:48)])
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


names(mb1.nexdat)

mb1.nexrf.dat<-na.exclude(mb1.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(mb1.rf.dat)
form<- Class~.
mb1.nexrf <- randomForest(form, data=mb1.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb1.nexrf), 2)
print(mb1.nexrf)
plot(mb1.nexrf ) #


