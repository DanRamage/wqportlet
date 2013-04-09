cvlm<-function(data,form,folds=10,whsize=10,xlim=NULL, ylim=NULL, abv=NULL, abh=NULL){ 
	len<-nrow(data)
	pred<-data.frame(0,0)
	names(pred)<- c('observed', 'predicted')
	
	dev.off(2)			
	for (i in 1:folds){
		set.seed(i*25)
		testdat<-data[-sample(1:len, whsize),]
		set.seed(i*25)
		valdat<-data[sample(1:len, whsize),]
		fit<-lm(form, data=testdat)
		pred1<-data.frame(log10(valdat$etcoc), predict(fit, valdat))
			names(pred1)<-c('observed', 'predicted')
		pred<-rbind(pred, pred1)
		print(pred)
		print(cor.test(pred[,2], pred[,1]))
		}
	
	pred<-pred[-1,]
	pred
	cor.test(pred$predicted, pred$observed)
	par(pty='s')
	plot(pred[,2], pred[,1], 
		xlim=xlim, ylim=xlim, 
		xlab='Predicted median logfc', ylab='Observed median logfc')
	abline(v=abv, lty=6)
	abline(h=abh, lty=6)
	
}
plot(nmb2.regdat$tide)

#example usage:

form<-log10(etcoc)~radar_rain_summary_24+range+predsal+tide
whsize<-nrow(nmb2.regdat)*.1

cvlm(data=nmb2.regdat, form=form, folds=0, whsize=whsize, xlim=c(0,5), ylim=c(0,5) )

