#############################################################################################
#			GC region.
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
                  rain_gauge = 'gardcty' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
gc.dat.new<- fetch(RS)
names(gc.dat.new)

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
gc.dat.new$tide<- as.factor(gc.dat.new$tide)
gc.dat.new$weather<-as.factor(gc.dat.new$weather)
gc.dat.new$station<-as.factor(gc.dat.new$station)

#extract date from DateTime
gc.dat.new$dateT<-gc.dat.new$date
gc.dat.new$date<-as.Date(substr(gc.dat.new$dateT,1, 10))
# add year
gc.dat.new$year<-as.numeric(substring(gc.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

gc.dat.new$Class<-as.factor(1)
gc.dat.new$Class<-ifelse(gc.dat.new$etcoc>=104, 2, gc.dat.new$Class)
gc.dat.new$Class<-ifelse(gc.dat.new$etcoc>=500, 3, gc.dat.new$Class)
gc.dat.new$Class<-ordered(gc.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

gc.dat.new<-merge(gc.dat.new, temp, by="date", all.x=T, all.y=F)
names(gc.dat.new)
gc.dat.new$range<-gc.dat.new$highFt - gc.dat.new$lowFt

gc.dat.new$nos8661070_wind_dir<-as.factor(gc.dat.new$nos8661070_wind_dir)
gc.dat.new$sun2_wind_dir<-as.factor(gc.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(gc.dat.new)
gc.regdat<-gc.dat.new
names(gc.regdat)

#create intervention variable
gc.regdat$intervention<-ifelse(gc.regdat$date<"2004-05-01", "before", "after")
gc.regdat$intervention<-ordered(gc.regdat$intervention, levels=c("before", "after"))


names(gc.regdat)
gc.olddat<-na.exclude(gc.regdat[,c(1:22, 44:49)])
nrow(gc.olddat) #n=610

gc.nexdat<-na.exclude(gc.regdat[,c(1:34, 44:49)])
#gc.nexdat<-gc.nexdat[gc.nexdat$tide!="4100",]
nrow(gc.nexdat) #n=376

gc.oosdat<-na.exclude(gc.regdat)
nrow(gc.oosdat) # n=131      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length

png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCDataDate.png"
		, bg="transparent")

plot(gc.regdat$date, log10(gc.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("Garden City Bacteria data")

dev.off()



#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


gc.oldreg<-lm(log10(etcoc)~
#	intervention
	+ station
	+ salinity
	+ tide
	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
	+ rain_summary_168
	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ weather
#	+ moon_phase
	+ range
	+ lowFt 
	, data=gc.olddat
	, na.action=na.omit

	)

summary(gc.oldreg)
AIC(gc.oldreg)
#plot(gc.oldreg)

#library(car)
vif(gc.oldreg)

qq.plot(gc.oldreg)

library(MASS)
sresid <- studres(gc.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(gc.olddat)
form<-log10(etcoc)~station+salinity+tide+rain_summary_24+rain_summary_168+rainfall_intensity_24+weather+range+lowFt
whsize<-nrow(gc.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCOldRegCV.png"
		, bg="transparent")
	cvlm(data=gc.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("GC 'Old' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(gc.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(gc.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCOldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('GC "Old Data" ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("gc.oldreg")
summary(gc.oldreg)
print(paste("AIC = ", AIC(gc.oldreg), sep=""))
print("CVr = 0.626")
sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

gc.nexreg<-lm(log10(etcoc)~
# 	+ intervention
	+ station
	+ salinity
	+ tide
#	+ rain_summary_24	
#	+ rain_summary_48	
#	+ rain_summary_96
#	+ rain_summary_120
#	+ rain_summary_144
	+ rain_summary_168
#	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ radar_rain_summary_24	
#	+ radar_rain_summary_48	
#	+ radar_rain_summary_96
	+ radar_rain_summary_120
#	+ radar_rain_summary_144
#	+ radar_rain_summary_168
	+ radar_rainfall_intensity_24
	+ radar_preceding_dry_day_cnt 
	+ weather
#	+ moon_phase
	+ range
	+ lowFt 
	, data=gc.nexdat
	, na.action=na.omit

	)

summary(gc.nexreg)
AIC(gc.nexreg)
#plot(gc.nexreg)

#library(car)
vif(gc.nexreg)


#library(MASS)
sresid <- studres(gc.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 


#     Validation               

names(gc.nexdat)
form<-log10(etcoc)~station+salinity+tide+rain_summary_168+radar_rain_summary_24+radar_rain_summary_120+radar_rainfall_intensity_24+radar_preceding_dry_day_cnt+weather+range+lowFt
whsize<-nrow(gc.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCNexRegCV.png"
		, bg="transparent")

cvlm(data=gc.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,5), whsize=whsize)
	abline(a=0, b=1)
	title("GC 'Nexrad' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(gc.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(gc.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCNexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('GC NEXRAD Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("gc.nexreg")
summary(gc.nexreg)
print(paste("AIC = ", AIC(gc.nexreg), sep=""))
print("CVr = 0.624")
sink()

#################  Left off 08/22/2010

#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(gc.oosdat)
gc.oosdat$intervention
gc.oosdat$nos8661070_wind_dir<-factor(gc.oosdat$nos8661070_wind_dir)

gc.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
#	 station
#	- 1
	+ salinity
#	+ tide
	+ radar_rain_summary_24	
#	+ radar_preceding_dry_day_cnt 
#	+ weather
#	+ moon_phase
	+ range
#	+ lowFt 
#	+ sun2_wind_speed
#	+ sun2_wind_dir
	+ sun2_water_temp
	+ sun2_salinity
#	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
	+ nos8661070_water_temp
	+ nos8661070_water_level
	, data=gc.oosdat
	, na.action=na.omit

	)

summary(gc.oosreg)
AIC(gc.oosreg)
#plot(gc.oosreg)

#library(car)
vif(gc.oosreg)

cor.test(gc.oosdat$nos8661070_wind_spd, gc.oosdat$sun2_wind_speed)
cor.test(gc.oosdat$salinity, gc.oosdat$sun2_salinity)

qq.plot(gc.oosreg)

#library(MASS)
sresid <- studres(gc.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(gc.oosdat)

gc.oosreg$call
form<-log10(etcoc) ~ +salinity + radar_rain_summary_24 + range + sun2_water_temp + sun2_salinity + nos8661070_water_temp + nos8661070_water_level
whsize<-nrow(gc.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCIOOSRegCV.png"
		, bg="transparent")

cvlm(data=gc.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("GC 'IOOS' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(gc.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(gc.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCOosRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('GC IOOS Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("gc.oosreg")
summary(gc.oosreg)
print(paste("AIC = ", AIC(gc.oosreg), sep=""))
print("CVr = 0.761")
sink()

############################# Develop CART Models

library(rpart)
names(gc.regdat)

boxplot(gc.regdat$radar_rain_summary_24 ~ gc.regdat$Class)
boxplot(gc.regdat$salinity ~ gc.regdat$Class)
boxplot(gc.regdat$sun2_salinity ~ gc.regdat$Class) 

GCTreeOld<-rpart(Class~
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
	, data=gc.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeOld.png"
			, bg="transparent")
		plot(GCTreeOld)
		text(GCTreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("GC With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(GCTreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeOldCP.png")
		plotcp(GCTreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
GCTreeOld.prune<-prune(GCTreeOld, cp=.019)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeOldPrune.png")
	plot(GCTreeOld.prune)
	text(GCTreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(GCTreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(GCTreeOld.prune, gc.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(gc.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: GC, 'Old' data")
	table(pred[,4:5])
sink()


################################### NEXRAD DATA Tree

GCTreeNex<-rpart(Class~
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
	, data=gc.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeNex.png"
			, bg="transparent")
		plot(GCTreeNex)
		text(GCTreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("GC With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(GCTreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeNexCP.png")
		plotcp(GCTreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
GCTreeNex.prune<-prune(GCTreeNex, cp=.012)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeNexPrune.png")
	plot(GCTreeNex.prune)
	text(GCTreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("GC 'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(GCTreeNex.prune)
	

pred<-as.data.frame(predict(GCTreeNex.prune, gc.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(gc.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: GC, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(gc.oosdat)

GCTreeOOS<-rpart(Class~
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
	, data=gc.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeOOS.png"
			, bg="transparent")
		plot(GCTreeOOS)
		text(GCTreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("GC With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(GCTreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeOOSCP.png")
		plotcp(GCTreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info

#########    NA for this region/series

GCTreeOOS.prune<-GCTreeOOS
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/GCTreeOOSPrune.png")
	plot(GCTreeOOS.prune)
	text(GCTreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("GC 'IOOS' Data Pruned Tree")
	dev.off()
	summary(GCTreeOOS.prune)
	

pred<-as.data.frame(predict(GCTreeOOS.prune, gc.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(gc.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/GC/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: GC, 'IOOS' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(gc.olddat)

gc.rf.dat<-na.exclude(gc.olddat[, c(3, 6:20, 22, 24:28)])
#gc.rf.dat$Class<-ordered(gc.rf.dat$Class, levels = c("Low", "Medium"))
unique(gc.rf.dat$Class)
names(gc.rf.dat)
form<- Class~.
gc.rf <- randomForest(form, data=gc.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(gc.rf), 2)
print(gc.rf)
plot(gc.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(gc.regdat)

dat2<-na.exclude(gc.regdat[,c(3, 6:8, 45:48)])
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


names(gc.nexdat)

gc.nexrf.dat<-na.exclude(gc.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(gc.rf.dat)
form<- Class~.
gc.nexrf <- randomForest(form, data=gc.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(gc.nexrf), 2)
print(gc.nexrf)
plot(gc.nexrf ) #


