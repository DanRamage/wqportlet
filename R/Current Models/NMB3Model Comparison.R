#############################################################################################
#			NMB3 region.
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
                  rain_gauge = 'nmb3' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
nmb3.dat.new<- fetch(RS)
names(nmb3.dat.new)

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
nmb3.dat.new$tide<- as.factor(nmb3.dat.new$tide)
nmb3.dat.new$weather<-as.factor(nmb3.dat.new$weather)
nmb3.dat.new$station<-as.factor(nmb3.dat.new$station)

#extract date from DateTime
nmb3.dat.new$dateT<-nmb3.dat.new$date
nmb3.dat.new$date<-as.Date(substr(nmb3.dat.new$dateT,1, 10))
# add year
nmb3.dat.new$year<-as.numeric(substring(nmb3.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

nmb3.dat.new$Class<-as.factor(1)
nmb3.dat.new$Class<-ifelse(nmb3.dat.new$etcoc>=104, 2, nmb3.dat.new$Class)
nmb3.dat.new$Class<-ifelse(nmb3.dat.new$etcoc>=500, 3, nmb3.dat.new$Class)
nmb3.dat.new$Class<-ordered(nmb3.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

nmb3.dat.new<-merge(nmb3.dat.new, temp, by="date", all.x=T, all.y=F)
names(nmb3.dat.new)
nmb3.dat.new$range<-nmb3.dat.new$highFt - nmb3.dat.new$lowFt

nmb3.dat.new$nos8661070_wind_dir<-as.factor(nmb3.dat.new$nos8661070_wind_dir)
nmb3.dat.new$sun2_wind_dir<-as.factor(nmb3.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(nmb3.dat.new)
nmb3.regdat<-nmb3.dat.new
names(nmb3.regdat)

#create intervention variable
nmb3.regdat$intervention<-ifelse(nmb3.regdat$date<"2004-05-01", "before", "after")
nmb3.regdat$intervention<-ordered(nmb3.regdat$intervention, levels=c("before", "after"))


names(nmb3.regdat)
nmb3.olddat<-na.exclude(nmb3.regdat[,c(1:22, 44:49)])
nrow(nmb3.olddat) #n=491

nmb3.nexdat<-na.exclude(nmb3.regdat[,c(1:34, 44:49)])
nmb3.nexdat<-nmb3.nexdat[nmb3.nexdat$tide!="4100",]
nrow(nmb3.nexdat) #n=330

nmb3.oosdat<-na.exclude(nmb3.regdat)
nrow(nmb3.oosdat) # n=149      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3DataDate.png"
		, bg="transparent")
plot(nmb3.regdat$date, log10(nmb3.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("NMB3 Bacteria data")
dev.off()



#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


nmb3.oldreg<-lm(log10(etcoc)~
#	intervention
	+ station
	+ salinity
#	+ tide
	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
#	+ rain_summary_168
#	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ weather
	+ moon_phase
	+ range
#	+ lowFt 
	, data=nmb3.olddat
	, na.action=na.omit

	)


summary(nmb3.oldreg)
AIC(nmb3.oldreg)
#plot(nmb3.oldreg)


#library(car)
vif(nmb3.oldreg)

qq.plot(nmb3.oldreg)

library(MASS)
sresid <- studres(nmb3.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(nmb3.olddat)
form<-log10(etcoc)~intervention+station+salinity+tide+preceding_dry_day_count+moon_phase+weather
whsize<-nrow(nmb3.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3OldRegCV.png"
		, bg="transparent")
	cvlm(data=nmb3.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("NMB3 'Old' Regression Model CV Results")

dev.off()

#      ROC Curve 
library(ROCR)
labels<-ordered(ifelse(nmb3.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(nmb3.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3OldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('NMB3 "Old Data" ROC Curve')
dev.off()



sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("nmb3.oldreg")
summary(nmb3.oldreg)
print("AIC = ")
AIC(nmb3.oldreg)
print("CVr = 0.701")
sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

nmb3.nexreg<-lm(log10(etcoc)~
 	+ intervention
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
	+ moon_phase
#	+ range
#	+ lowFt 
	, data=nmb3.nexdat
	, na.action=na.omit

	)

summary(nmb3.nexreg)
AIC(nmb3.nexreg)
#plot(nmb3.nexreg)


#library(car)
vif(nmb3.nexreg)

qq.plot(nmb3.nexreg)

library(MASS)
sresid <- studres(nmb3.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(nmb3.nexdat)
form<-log10(etcoc)~intervention+station+salinity+radar_rain_summary_24+weather+moon_phase
whsize<-nrow(nmb3.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3NexRegCV.png"
		, bg="transparent")

cvlm(data=nmb3.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,5), whsize=whsize)
	abline(a=0, b=1)
	title("NMB3 'Nexrad' Regression Model CV Results")

dev.off()

#      ROC Curve 
library(ROCR)
labels<-ordered(ifelse(nmb3.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(nmb3.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3NexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('NMB3 NEXRAD Data ROC Curve')
dev.off()



sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("nmb3.nexreg") 
summary(nmb3.nexreg)
print("AIC = ") 
AIC(nmb3.nexreg)
print("CVr = 0.751")

sink()


#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(nmb3.oosdat)
nmb3.oosdat$intervention
nmb3.oosdat$nos8661070_wind_dir<-factor(nmb3.oosdat$nos8661070_wind_dir)

nmb3.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
#	 station
#	- 1
	+ salinity
	+ tide
	+ radar_rain_summary_24	
#	+ radar_preceding_dry_day_cnt 
	+ weather
	+ moon_phase
#	+ range
#	+ lowFt 
	+ sun2_wind_speed
#	+ sun2_wind_dir
	+ sun2_water_temp
	+ sun2_salinity
	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
#	+ nos8661070_water_temp
#	+ nos8661070_water_level
	, data=nmb3.oosdat
	, na.action=na.omit

	)

summary(nmb3.oosreg)
AIC(nmb3.oosreg)
#plot(nmb3.oosreg)

#library(car)
vif(nmb3.oosreg)

cor.test(nmb3.oosdat$nos8661070_wind_spd, nmb3.oosdat$sun2_wind_speed)
cor.test(nmb3.oosdat$salinity, nmb3.oosdat$sun2_salinity)

qq.plot(nmb3.oosreg)

library(MASS)
sresid <- studres(nmb3.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(nmb3.oosdat)

nmb3.oosreg$call
form<-log10(etcoc) ~ salinity + tide + radar_rain_summary_24 + weather + moon_phase + sun2_wind_speed + sun2_water_temp + sun2_salinity + nos8661070_wind_spd
whsize<-nrow(nmb3.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3IOOSRegCV.png"
		, bg="transparent")

cvlm(data=nmb3.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("NMB3 'IOOS' Regression Model CV Results")

dev.off()

#      ROC Curve 
library(ROCR)
labels<-ordered(ifelse(nmb3.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(nmb3.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3oosRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('NMB3 IOOS Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("nmb3.oosreg")
summary(nmb3.oosreg)
print("AIC = ") 
AIC(nmb3.oosreg)
print("CVr = 0.717")

sink()

############################# Develop CART Models

library(rpart)
names(nmb3.regdat)

boxplot(nmb3.regdat$radar_rain_summary_24 ~ nmb3.regdat$Class)
boxplot(nmb3.regdat$salinity ~ nmb3.regdat$Class)
boxplot(nmb3.regdat$sun2_salinity ~ nmb3.regdat$Class) 

NMB3TreeOld<-rpart(Class~
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
	, data=nmb3.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeOld.png"
			, bg="transparent")
		plot(NMB3TreeOld)
		text(NMB3TreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("NMB3 With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(NMB3TreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeOldCP.png")
		plotcp(NMB3TreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
NMB3TreeOld.prune<-prune(NMB3TreeOld, cp=.03)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeOldPrune.png")
	plot(NMB3TreeOld.prune)
	text(NMB3TreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(NMB3TreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(NMB3TreeOld.prune, nmb3.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(nmb3.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: NMB3, 'Old' data")
	table(pred[,4:5])
sink()

################################### NEXRAD DATA Tree

NMB3TreeNex<-rpart(Class~
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
	, data=nmb3.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeNex.png"
			, bg="transparent")
		plot(NMB3TreeNex)
		text(NMB3TreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("NMB3 With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(NMB3TreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeNexCP.png")
		plotcp(NMB3TreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
NMB3TreeNex.prune<-prune(NMB3TreeNex, cp=.015)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeNexPrune.png")
	plot(NMB3TreeNex.prune)
	text(NMB3TreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(NMB3TreeNex.prune)
	

pred<-as.data.frame(predict(NMB3TreeNex.prune, nmb3.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(nmb3.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: NMB3, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(nmb3.oosdat)

NMB3TreeOOS<-rpart(Class~
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
	, data=nmb3.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeOOS.png"
			, bg="transparent")
		plot(NMB3TreeOOS)
		text(NMB3TreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("NMB3 With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(NMB3TreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeOOSCP.png")
		plotcp(NMB3TreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info
NMB3TreeOOS.prune<-prune(NMB3TreeOOS, cp=.01)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/NMB3TreeOOSPrune.png")
	plot(NMB3TreeOOS.prune)
	text(NMB3TreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'IOOS' Data Pruned Tree")
	dev.off()
	summary(NMB3TreeOOS.prune)
	

pred<-as.data.frame(predict(NMB3TreeOOS.prune, nmb3.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(nmb3.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/NMB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: NMB3, 'NEXRAD' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(nmb3.olddat)

nmb3.rf.dat<-na.exclude(nmb3.olddat[, c(3, 6:20, 22, 24:28)])
#nmb3.rf.dat$Class<-ordered(nmb3.rf.dat$Class, levels = c("Low", "Medium"))
unique(nmb3.rf.dat$Class)
names(nmb3.rf.dat)
form<- Class~.
nmb3.rf <- randomForest(form, data=nmb3.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(nmb3.rf), 2)
print(nmb3.rf)
plot(nmb3.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(nmb3.regdat)

dat2<-na.exclude(nmb3.regdat[,c(3, 6:8, 45:48)])
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


names(nmb3.nexdat)

nmb3.nexrf.dat<-na.exclude(nmb3.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(nmb3.rf.dat)
form<- Class~.
nmb3.nexrf <- randomForest(form, data=nmb3.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(nmb3.nexrf), 2)
print(nmb3.nexrf)
plot(nmb3.nexrf ) #


