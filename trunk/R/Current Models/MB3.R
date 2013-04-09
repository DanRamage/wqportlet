#############################################################################################
#			MB3 region.
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
                  rain_gauge = 'mb3' 
                  ORDER BY date ASC;" )

#Get the data from the results set. 
mb3.dat.new<- fetch(RS)
names(mb3.dat.new)

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
mb3.dat.new$tide<- as.factor(mb3.dat.new$tide)
mb3.dat.new$weather<-as.factor(mb3.dat.new$weather)
mb3.dat.new$station<-as.factor(mb3.dat.new$station)

#extract date from DateTime
mb3.dat.new$dateT<-mb3.dat.new$date
mb3.dat.new$date<-as.Date(substr(mb3.dat.new$dateT,1, 10))
# add year
mb3.dat.new$year<-as.numeric(substring(mb3.dat.new$dateT, 1,4))



# add factor variable for concentration category: "low", "medium", "high"
# Low < 104cfu/100ml; 104=< Med < 500; High > 500

mb3.dat.new$Class<-as.factor(1)
mb3.dat.new$Class<-ifelse(mb3.dat.new$etcoc>=104, 2, mb3.dat.new$Class)
mb3.dat.new$Class<-ifelse(mb3.dat.new$etcoc>=500, 3, mb3.dat.new$Class)
mb3.dat.new$Class<-ordered(mb3.dat.new$Class, levels=c(1,2,3), labels=c("Low", "Medium", "High"))

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

mb3.dat.new<-merge(mb3.dat.new, temp, by="date", all.x=T, all.y=F)
names(mb3.dat.new)
mb3.dat.new$range<-mb3.dat.new$highFt - mb3.dat.new$lowFt

mb3.dat.new$nos8661070_wind_dir<-as.factor(mb3.dat.new$nos8661070_wind_dir)
mb3.dat.new$sun2_wind_dir<-as.factor(mb3.dat.new$sun2_wind_dir)
#########################################################################
			##### Regression modeling

# signif corr vars: all rainfall except prev dry day (higher for NEXRAD); 
# salinity (IOOS and sampled(Sampled has much higher r));
# moonphase, tide, weather, lowFt

###############################  Regression Model Data
names(mb3.dat.new)
mb3.regdat<-mb3.dat.new
names(mb3.regdat)

#create intervention variable
mb3.regdat$intervention<-ifelse(mb3.regdat$date<"2004-05-01", "before", "after")
mb3.regdat$intervention<-ordered(mb3.regdat$intervention, levels=c("before", "after"))


names(mb3.regdat)
mb3.olddat<-na.exclude(mb3.regdat[,c(1:22, 44:49)])
nrow(mb3.olddat) #n=616

mb3.nexdat<-na.exclude(mb3.regdat[,c(1:34, 44:49)])
#mb3.nexdat<-mb3.nexdat[mb3.nexdat$tide!="4100",]
nrow(mb3.nexdat) #n=314

mb3.oosdat<-na.exclude(mb3.regdat)
nrow(mb3.oosdat) # n=66      most missing data is from Sun2; sun2 has salinity data, other OS data does not 
			# removing traditional variables didn't extend data set length

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3DataDate.png"
		, bg="transparent")

plot(mb3.regdat$date, log10(mb3.regdat$etcoc), xlab="Date", ylab="log10 enterococci", xlim=c(as.Date("2001-01-01"), as.Date("2009-12-31")), ylim=c(1,5))
	title("MB3 Bacteria data")

dev.off()

#################### "OLD" Model ###############################
#
#    Model selection using data avail for "old" variables
#
################################################################


mb3.oldreg<-lm(log10(etcoc)~
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
	+ rainfall_intensity_24
#	+ preceding_dry_day_count 
	+ weather
#	+ moon_phase
#	+ range
#	+ lowFt 
	, data=mb3.olddat
	, na.action=na.omit

	)

summary(mb3.oldreg)
AIC(mb3.oldreg)
#plot(mb3.oldreg)
anova(mb3.oldreg)
#library(car)
vif(mb3.oldreg)

qq.plot(mb3.oldreg)

library(MASS)
sresid <- studres(mb3.oldreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb3.olddat)
form<-log10(etcoc)~station+salinity+rain_summary_24+rainfall_intensity_24+weather
whsize<-nrow(mb3.olddat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3OldRegCV.png"
		, bg="transparent")
	cvlm(data=mb3.olddat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
		abline(a=0, b=1)
	title("MB3 'Old' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb3.olddat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb3.oldreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3OldRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB3 "Old Data" ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb3.oldreg")
summary(mb3.oldreg)
print(paste("AIC = ", AIC(mb3.oldreg), sep = ""))
print ("CVr = 0.660")
sink()

#################### "NEXRAD" Model ###############################
#
#    Model selection using data avail for "old" variables and NEXRAD
#
################################################################

mb3.nexreg<-lm(log10(etcoc)~
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
	, data=mb3.nexdat
	, na.action=na.omit

	)

summary(mb3.nexreg)
AIC(mb3.nexreg)
#plot(mb3.nexreg)

#library(car)
vif(mb3.nexreg)


#library(MASS)
sresid <- studres(mb3.nexreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb3.nexdat)
form<-log10(etcoc)~intervention+station+salinity+tide+radar_rain_summary_24+weather
whsize<-nrow(mb3.nexdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3NexRegCV.png"
		, bg="transparent")

cvlm(data=mb3.nexdat, form=form, folds=10, xlim=c(0,5), ylim=c(0,5), whsize=whsize)
	abline(a=0, b=1)
	title("MB3 'Nexrad' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb3.nexdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb3.nexreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3NexRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB3 NEXRAD Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb3.nexreg")
summary(mb3.nexreg)
print(paste("AIC = ", AIC(mb3.nexreg), sep=""))
print("CVr = 0.641")
sink()



#################### "IOOS" Model ###############################
#
#    Model selection using data avail for all variables 
#
################################################################

names(mb3.oosdat)
mb3.oosdat$intervention
mb3.oosdat$nos8661070_wind_dir<-factor(mb3.oosdat$nos8661070_wind_dir)

mb3.oosreg<-lm(log10(etcoc)~
	# intervention: no before data, only one level of the factor
#	 station
#	- 1
	+ salinity
#	+ tide
#	+ radar_rain_summary_24	
#	+ radar_preceding_dry_day_cnt 
#	+ weather
	+ moon_phase
	+ range
#	+ lowFt 
#	+ sun2_wind_speed
#	+ sun2_wind_dir
#	+ sun2_water_temp
#	+ sun2_salinity
#	+ nos8661070_wind_spd
#	+ nos8661070_wind_dir	
#	+ nos8661070_water_temp
#	+ nos8661070_water_level
	, data=mb3.oosdat
	, na.action=na.omit

	)

summary(mb3.oosreg)
AIC(mb3.oosreg)
#plot(mb3.oosreg)

#library(car)
vif(mb3.oosreg)

cor.test(mb3.oosdat$nos8661070_wind_spd, mb3.oosdat$sun2_wind_speed)
cor.test(mb3.oosdat$salinity, mb3.oosdat$sun2_salinity)

qq.plot(mb3.oosreg)

#library(MASS)
sresid <- studres(mb3.oosreg)
hist(sresid, freq=F,
   main="Distribution of Studentized Residuals")
xfit<-seq(min(na.exclude(sresid)),max(na.exclude(sresid)),length=length(sresid))
yfit<-dnorm(xfit)
lines(xfit, yfit) 

#     Validation               

names(mb3.oosdat)

mb3.oosreg$call
form<-log10(etcoc) ~ salinity + moon_phase + range
whsize<-nrow(mb3.oosdat)*.2

png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3IOOSRegCV.png"
		, bg="transparent")

cvlm(data=mb3.oosdat, form=form, xlim=c(0,5), ylim=c(0,5), folds=10, whsize=whsize)
	abline(a=0, b=1)
	title("MB3 'IOOS' Regression Model CV Results")

dev.off()

##             ROC CURVES

library(ROCR)
labels<-ordered(ifelse(mb3.oosdat$etcoc>104, 1, 0), levels=c(0,1))  # 1 = positive test, 0 = negative test
plot(labels) # to confirm label definition
pred<-prediction(mb3.oosreg$fitted.values, labels)
perf<-performance(pred, "tpr", "fpr")
par(pty="s")
png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3IOOSRegROC.png"
		, bg="transparent")
	plot(perf, colorize=T, pty="s", print.cutoffs.at=c(log10(104)))
	title('MB3 IOOS Data ROC Curve')
dev.off()


sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/RegressionEquations.txt", append = T, type = "output",
    split = T)
print("mb3.oosreg")
summary(mb3.oosreg)
print(paste("AIC = ", AIC(mb3.oosreg), sep=""))
print("CVr = 0.753")
sink()

############################# Develop CART Models

library(rpart)
names(mb3.regdat)

boxplot(mb3.regdat$radar_rain_summary_24 ~ mb3.regdat$Class)
boxplot(mb3.regdat$salinity ~ mb3.regdat$Class)
boxplot(mb3.regdat$sun2_salinity ~ mb3.regdat$Class) 

MB3TreeOld<-rpart(Class~
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
	, data=mb3.olddat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeOld.png"
			, bg="transparent")
		plot(MB3TreeOld)
		text(MB3TreeOld, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB3 With 'Old' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB3TreeOld)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeOldCP.png")
		plotcp(MB3TreeOld)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB3TreeOld.prune<-prune(MB3TreeOld, cp=.03)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeOldPrune.png")
	plot(MB3TreeOld.prune)
	text(MB3TreeOld.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("'Old' Data Pruned Tree")
	summary(MB3TreeOld.prune)
	dev.off()
	

pred<-as.data.frame(predict(MB3TreeOld.prune, mb3.olddat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb3.olddat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB3, 'Old' data")
	table(pred[,4:5])
sink()


################################### NEXRAD DATA Tree

MB3TreeNex<-rpart(Class~
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
	, data=mb3.nexdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeNex.png"
			, bg="transparent")
		plot(MB3TreeNex)
		text(MB3TreeNex, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB3 With 'NEXRAD' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB3TreeNex)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeNexCP.png")
		plotcp(MB3TreeNex)
	dev.off()

		

# Adjust Tree based on diagnostic info
MB3TreeNex.prune<-prune(MB3TreeNex, cp=.049)
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeNexPrune.png")
	plot(MB3TreeNex.prune)
	text(MB3TreeNex.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("MB3 'NEXRAD' Data Pruned Tree")
	dev.off()
	summary(MB3TreeNex.prune)
	

pred<-as.data.frame(predict(MB3TreeNex.prune, mb3.nexdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb3.nexdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB3, 'NEXRAD' data")
	table(pred[,4:5])
sink()

##################################### IOOS DATA #######################
names(mb3.oosdat)

MB3TreeOOS<-rpart(Class~
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
	, data=mb3.oosdat
	, na.action=na.omit
	)

	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeOOS.png"
			, bg="transparent")
		plot(MB3TreeOOS)
		text(MB3TreeOOS, cex=.75, pretty= '3', use.n=T, xpd=T)
		title("MB3 With 'IOOS' Variables")
	dev.off()
	
	sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
		printcp(MB3TreeOOS)
	sink()
	
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeOOSCP.png")
		plotcp(MB3TreeOOS)
	dev.off()

		

# Adjust Tree based on diagnostic info

#########    NA for this region/series

MB3TreeOOS.prune<-MB3TreeOOS
	png("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/MB3TreeOOSPrune.png")
	plot(MB3TreeOOS.prune)
	text(MB3TreeOOS.prune, cex=.75, all=T, pretty=3, use.n=T, xpd=T)
	title("MB3 'IOOS' Data Pruned Tree")
	dev.off()
	summary(MB3TreeOOS.prune)
	

pred<-as.data.frame(predict(MB3TreeOOS.prune, mb3.oosdat))
pred$predClass<-ifelse(pred$Medium>pred$Low & pred$Medium>pred$High, "Medium", "Low")
pred$predClass<-ifelse(pred$High>pred$Medium & pred$High>pred$Low,"High", pred$predClass)
pred$obsClass<-as.character(mb3.oosdat$Class)

pred[,4]<- ordered(pred[,4], levels=c("Low","Medium","High"))
pred[,5]<- ordered(pred[,5], levels=c("Low","Medium","High")) 
#pred[,4:5]

sink("C:/Heath/Projects/SCBeaches/RCode/Figures/MB3/Model selection/TreeSelection.txt", append = T, type = "output",
    		split = T)
	print("Prediction results: MB3, 'NEXRAD' data")
	table(pred[,4:5])
sink()


###############  RandomForest analysis


#################################################################
#										    #		
#                      Random forest old data			    #
#										    #	
################################################################# 

library(randomForest)
names(mb3.olddat)

mb3.rf.dat<-na.exclude(mb3.olddat[, c(3, 6:20, 22, 24:28)])
#mb3.rf.dat$Class<-ordered(mb3.rf.dat$Class, levels = c("Low", "Medium"))
unique(mb3.rf.dat$Class)
names(mb3.rf.dat)
form<- Class~.
mb3.rf <- randomForest(form, data=mb3.rf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb3.rf), 2)
print(mb3.rf)
plot(mb3.rf ) #




###########################################################################
#
#         Not sure what the value is here. Another tree for the ensemble?
#

#new tree based on most important variables Salinity, station,moon, and 4 tide  variables 
names(mb3.regdat)

dat2<-na.exclude(mb3.regdat[,c(3, 6:8, 45:48)])
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


names(mb3.nexdat)

mb3.nexrf.dat<-na.exclude(mb3.nexdat[, c(3, 6:20, 22:34, 36:40)])
names(mb3.rf.dat)
form<- Class~.
mb3.nexrf <- randomForest(form, data=mb3.nexrf.dat, importance=TRUE, proximity=TRUE)

round(importance(mb3.nexrf), 2)
print(mb3.nexrf)
plot(mb3.nexrf ) #


