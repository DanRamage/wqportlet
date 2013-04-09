#############################################################################################
#			NMB2 region.
#			Script to query database, condition data, and 
#			develop new models with IOOS data
#			
#			2010-06-03
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
                  rain_gauge = 'nmb2' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
nmb2.dat.new<- fetch(RS)
names(nmb2.dat.new)

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
nmb2.dat.new$tide<- as.factor(nmb2.dat.new$tide)
nmb2.dat.new$weather<-as.factor(nmb2.dat.new$weather)
nmb2.dat.new$station<-as.factor(nmb2.dat.new$station)

#extract date from DateTime
nmb2.dat.new$dateT<-nmb2.dat.new$date
nmb2.dat.new$date<-as.Date(substr(nmb2.dat.new$dateT,1, 10))
# add year
nmb2.dat.new$year<-as.numeric(substring(nmb2.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

nmb2.dat.new$Class<-as.factor(1)
nmb2.dat.new$Class<-ifelse(nmb2.dat.new$etcoc>=104, 2, nmb2.dat.new$Class)
nmb2.dat.new$Class<-ifelse(nmb2.dat.new$etcoc>=500, 3, nmb2.dat.new$Class)
nmb2.dat.new$Class<-ordered(nmb2.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

nmb2.dat.new<-merge(nmb2.dat.new, temp, by="date", all.x=T, all.y=F)
names(nmb2.dat.new)
nmb2.dat.new$range<-nmb2.dat.new$highFt - nmb2.dat.new$lowFt

nmb2.dat.new$nos8661070_wind_dir<-as.factor(nmb2.dat.new$nos8661070_wind_dir)
nmb2.dat.new$sun2_wind_dir<-as.factor(nmb2.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(nmb2.dat.new)
nmb2.regdat<-nmb2.dat.new
names(nmb2.regdat)

#create intervention variable
nmb2.regdat$intervention<-as.factor(ifelse(nmb2.regdat$year<2005, 
	"before", "after"))
names(nmb2.regdat)
nmb2.olddat<-na.exclude(nmb2.regdat[,c(1:22, 44:49)])
nrow(nmb2.olddat) #n=523

nmb2.nexdat<-na.exclude(nmb2.regdat[,c(1:34, 44:49)])
nmb2.nexdat<-nmb2.nexdat[nmb2.nexdat$tide!="4100",]
nrow(nmb2.nexdat) #n=374

nmb2.oosdat<-na.exclude(nmb2.regdat)
nrow(nmb2.oosdat) # n=128      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length


png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2DataDate.png"
		, bg="transparent")
plot(nmb2.regdat$date, log10(nmb2.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("NMB2 Bacteria data")

dev.off()

#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


nmb2.oldreg<-lm(log10(etcoc)~
	intervention
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
	+ preceding_dry_day_count 
	+ weather
	+ moon_phase
#	+ range
#	+ lowFt 
	, data=nmb2.olddat
	, na.action=na.omit

	)

summary(nmb2.oldreg)
AIC(nmb2.oldreg)
#plot(nmb2.oldreg)


#library(car)
vif(nmb2.oldreg)

qq.plot(nmb2.oldreg)

library(MASS)
sresid <- studres(nmb2.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(nmb2.olddat)
form<-log10(etcoc)~intervention+station+salinity+tide+preceding_dry_day_count+moon_phase+weather
whsize<-nrow(nmb2.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2OldRegCV.png"
		, bg="transparent")
	cvlm(data=nmb2.olddat, form=form, folds=10, whsize=whsize, xlim=c(0,5), ylim=c(0,5))
		abline(a=0, b=1)
	title("NMB2 'Old' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(nmb2.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(nmb2.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2OldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('NMB2 "Old Data" ROC Curve')
dev.off()

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
summary(nmb2.oldreg)
print("nmb2.oldreg") 
print("AIC = ") 
AIC(nmb2.oldreg)
print("CVr = 0.615")

sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

nmb2.nexreg<-lm(log10(etcoc)~
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
	+ radar_preceding_dry_day_cnt 
	+ weather
	+ moon_phase
#	+ range
#	+ lowFt 
	, data=nmb2.nexdat
	, na.action=na.omit

	)

summary(nmb2.nexreg)
AIC(nmb2.nexreg)
#plot(nmb2.nexreg)


#library(car)
vif(nmb2.nexreg)

qq.plot(nmb2.nexreg)

library(MASS)
sresid <- studres(nmb2.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               
# 	Cross validation

names(nmb2.nexdat)
form<-log10(etcoc)~intervention+station+salinity+tide+radar_rain_summary_24+radar_preceding_dry_day_cnt+weather+moon_phase
whsize<-nrow(nmb2.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2NexRegCV.png"
		, bg="transparent")

cvlm(data=nmb2.nexdat, form=form, folds=10, whsize=whsize, xlim=c(0,5), ylim=c(0,5))
	abline(a=0, b=1)
	title("NMB2 'Nexrad' Regression Model CV Results")

dev.off()

#	ROC Curve analysis
library(ROCR)
labels<-ordered(ifelse(nmb2.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(nmb2.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2NexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('NMB2 NEXRAD Data ROC Curve')
dev.off()

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
summary(nmb2.nexreg)
print("nmb2.nexreg") 
print("AIC = ") 
AIC(nmb2.nexreg)
print("CVr = 0.703")

sink()

#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(nmb2.oosdat)

nmb2.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
	 station
	+ salinity
	+ tide
	+ radar_rain_summary_24	
#	+ radar_preceding_dry_day_cnt 
#	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
	+ sun2_wind_speed
#	+ sun2_wind_dir
#	+ sun2_water_temp
#	+ sun2_salinity
#	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
	+ nos8661070_water_temp
#	+ nos8661070_water_level
	, data=nmb2.oosdat
	, na.action=na.omit

	)

summary(nmb2.oosreg)
AIC(nmb2.oosreg)
#plot(nmb2.oosreg)

#library(car)
vif(nmb2.oosreg)

qq.plot(nmb2.oosreg)

library(MASS)
sresid <- studres(nmb2.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(nmb2.oosdat)

nmb2.oosreg$call
form<-log10(etcoc) ~ station + salinity + tide + radar_rain_summary_24 + range + sun2_wind_speed + nos8661070_water_temp
whsize<-nrow(nmb2.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2IOOSRegCV.png"
		, bg="transparent")

cvlm(data=nmb2.oosdat, form=form, folds=10, whsize=whsize,xlim=c(0,5), ylim=c(0,5))
	abline(a=0, b=1)
	title("NMB2 'IOOS' Regression Model CV Results")

dev.off()

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
summary(nmb2.oosreg)
print("nmb2.oosreg") 
print("AIC = ") 
AIC(nmb2.oosreg)
print("CVr = 0.511")

sink()

#	ROC Curve analysis
library(ROCR)
labels<-ordered(ifelse(nmb2.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(nmb2.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2IOOSRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('NMB2 IOOS Data ROC Curve')
dev.off()



############################# Develop CART Models

library(rpart)
names(nmb2.regdat)

boxplot(nmb2.regdat$radar_rain_summary_24 ~ nmb2.regdat$Class)
boxplot(nmb2.regdat$salinity ~ nmb2.regdat$Class)
boxplot(nmb2.regdat$sun2_salinity ~ nmb2.regdat$Class) 

NMB2TreeOld<-rpart(Class~
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
	, data=nmb2.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeOld.png"
			, bg="transparent")
		plot(NMB2TreeOld)
		text(NMB2TreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("NMB2 With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(NMB2TreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeOldCP.png")
		plotcp(NMB2TreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
NMB2TreeOld.prune<-prune(NMB2TreeOld, cp=.035)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeOldPrune.png")
	plot(NMB2TreeOld.prune)
	text(NMB2TreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(NMB2TreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(NMB2TreeOld.prune, nmb2.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(nmb2.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: NMB2, 'Old' data")
	table(pred[,4:5])
sink()

################################### NEXRAD DATA Tree

NMB2TreeNex<-rpart(Class~
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
	, data=nmb2.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeNex.png"
			, bg="transparent")
		plot(NMB2TreeNex)
		text(NMB2TreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("NMB2 With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(NMB2TreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeNexCP.png")
		plotcp(NMB2TreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
NMB2TreeNex.prune<-prune(NMB2TreeNex, cp=.01)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeNexPrune.png")
	plot(NMB2TreeNex.prune)
	text(NMB2TreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(NMB2TreeNex.prune)
	

pred<-as.data.frame(predict(NMB2TreeNex.prune, nmb2.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(nmb2.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: NMB2, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(nmb2.oosdat)

NMB2TreeOOS<-rpart(Class~
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
	, data=nmb2.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeOOS.png"
			, bg="transparent")
		plot(NMB2TreeOOS)
		text(NMB2TreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("NMB2 With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(NMB2TreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeOOSCP.png")
		plotcp(NMB2TreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info
NMB2TreeOOS.prune<-prune(NMB2TreeOOS, cp=.01)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/NMB2TreeOOSPrune.png")
	plot(NMB2TreeOOS.prune)
	text(NMB2TreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'IOOS' Data Pruned Tree")
	dev.off()
	summary(NMB2TreeOOS.prune)
	

pred<-as.data.frame(predict(NMB2TreeOOS.prune, nmb2.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(nmb2.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB2/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: NMB2, 'NEXRAD' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(nmb2.olddat)

nmb2.rf.dat<-na.exclude(nmb2.olddat[, c(3, 6:20, 22, 24:28)])
#nmb2.rf.dat$Class<-ordered(nmb2.rf.dat$Class, levels = c("Low", "Medium"))
unique(nmb2.rf.dat$Class)
names(nmb2.rf.dat)
form<- Class~.
nmb2.rf <- randomForest(form, data=nmb2.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(nmb2.rf), 2)
print(nmb2.rf)
plot(nmb2.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(nmb2.regdat)

dat2<-na.exclude(nmb2.regdat[,c(3, 6:8, 45:48)])
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


names(nmb2.nexdat)

nmb2.nexrf.dat<-na.exclude(nmb2.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(nmb2.rf.dat)
form<- Class~.
nmb2.nexrf <- randomForest(form, data=nmb2.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(nmb2.nexrf), 2)
print(nmb2.nexrf)
plot(nmb2.nexrf ) #


