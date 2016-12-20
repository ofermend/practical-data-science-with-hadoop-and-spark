#
example_data <- read.table("example.data", header=T, sep="\t")
max_y <- max(example_data$Iced_Coffee,example_data$Hot_Coffee,example_data$Hot_Tea)
xmonth<-example_data[,c('Month')]
# remove the months column and create matrix
example_data.mat<-as.matrix(example_data[-1])
 
# Line Chart
 png(filename="./line_chart.png", height=600, width=600, bg="white")
 plot(example_data$Iced_Coffee, type="o", ylim=c(0,max_y),axes=FALSE, ann=FALSE)
 box()
 title(ylab= "Iced_Coffee")
 title(xlab= "Month")
 title(main="Line Chart")
 axis(2, las=1, at=4*0:max_y)
 axis(1, at=1:12, lab=xmonth)
 dev.off()
# Multiple Line Chart
 png(filename="./multi_line_chart.png", height=600, width=600, bg="white")
 plot(example_data$Iced_Coffee, type="o", ylim=c(0,max_y),axes=FALSE, ann=FALSE)
 box()
 title(ylab= "Total")
 title(xlab= "Month")
 title(main= "Multi Line Chart")
 axis(2, las=1, at=4*0:max_y)
 axis(1, at=1:12, lab=xmonth)
lines(example_data$Hot_Coffee, type="o", pch=22, lty=2)
lines(example_data$Hot_Tea, type="o", pch=23, lty=3)
legend(1, max_y,c("Iced_Coffee","Hot_Coffee","Hot_Tea"), pch=21:23, lty=1:3)
dev.off()

# bar chart
png(filename="./bar_chart.png", height=600, width=600, bg="white")
barplot(example_data$Iced_Coffee, ylim=c(0,max_y), xlab="Month", ylab="Total: Iced Coffee", names.arg=xmonth, density=c(10,20,30,40,50,60),main="Bar Chart")
box()
dev.off()
# multiple bar chart
 png(filename="./multi_bar_chart.png", height=600, width=600, bg="white")
barplot(example_data.mat, ylim=c(0,1.1*max_y),xlab="Item", ylab="Total", beside=TRUE, density=c(10,20,30,40,50,60),main="Mutliple Bar Chart")
legend("topleft",legend=xmonth, density=c(10,20,30,4050,60),bty="n")
box()
dev.off()
# stacked bar
 png(filename="./stacked_bar_chart.png", height=600, width=600, bg="white")
barplot(t(example_data.mat), ylim=c(0,40),ylab="Total", space=0.1, cex.axis=0.8, las=1,xlab="Month" ,names.arg=xmonth, cex=0.8, main="Stacked Bar Chart",density=c(20,30,40))
legend("topleft",c("Iced_Coffee","Hot_Coffee","Hot_Tea"), density=c(20,30,40),bty="n")
box()
dev.off()

# 100% stacked bar
 png(filename="./100stacked_bar_chart.png", height=600, width=600, bg="white")
prop_example_matrix <- (prop.table(example_data.mat,1))
barplot(t(prop_example_matrix), ylab="Total", ylim=c(0,1.2), space=0.1, cex.axis=0.8, las=1,xlab="Month" ,names.arg=xmonth, cex=0.8, main="100% Stacked Bar Chart",density=c(20,30,40))
box()
legend("topleft",c("Iced_Coffee","Hot_Coffee","Hot_Tea"), density=c(20,30,40),bty="n")
dev.off()

# Histogram
 png(filename="./single_historgram.png", height=600, width=600, bg="white")
hist(example_data$Iced_Coffee,  col="gray",ann=FALSE)
title(xlab= "Number of Cups")
title(ylab= "Frequency of Iced Coffee Purchased")
title(main= "Single Variable Histogram")
box()
dev.off()

# Pie
 png(filename="./pie_chart.png", height=600, width=600, bg="white")
pie(example_data$Iced_Coffee, labels=xmonth,density=c(17,3,9,6,12,3,15,7,16,5,10), main="Pie Chart\nIced Coffee")
box()
dev.off()

#Dot Chart
 png(filename="./dot_chart.png", height=600, width=600, bg="white")
dotchart(head(t(example_data.mat),2),pch = 19,cex=0.9)
#dotchart(t(example_data.mat))
 title(xlab= "Value")
 title(main= "Dot Chart")
dev.off()


# Scatter Chart
 png(filename="./scatter_chart.png", height=600, width=600, bg="white")

 plot(example_data$Iced_Coffee,example_data$Hot_Coffee,ann=FALSE, cex=1.4,pch=19)
 title(main="Scatter Chart")
 title(ylab= "Hot_Coffee")
 title(xlab= "Iced_Coffee")
box()
dev.off()

# Scater matrix
 png(filename="./Scatter_Matrix.png", height=600, width=600, bg="white")
 plot(example_data[,2:4], pch=19)
title(main="Scatter Plot Matrix",line = +3)
dev.off()

# bubble chart
 png(filename="./bubble_chart.png", height=600, width=600, bg="white")
radius <- sqrt( example_data$Hot_Tea)
symbols(example_data$Iced_Coffee,example_data$Hot_Coffee,circles=radius,inches=0.35, bg="gray",ann=FALSE)
 title(main="Bubble Chart\nDiameter~Hot_Tea")
 title(ylab= "Hot_Coffee")
 title(xlab= "Iced_Coffee")
dev.off()

# Stacked Area (requires ggplot)
library(ggplot2)
example_data <- read.table("example2.data", header=T, sep="\ ")
png(filename="./stacked_area_chart.png", height=600, width=600, bg="white")
print(ggplot(example_data, aes(x=date,y=sales,group=product,fill=product)) + geom_area(position="fill")+
ggtitle("Stacked Area Chart")+
scale_fill_grey(start = .3, end = .8)+theme_bw())
dev.off()

# Boxplot
example_data <- read.table("example2.data", header=T, sep="\ ")
png(filename="./box_chart.png", height=600, width=600, bg="white")
boxplot(sales~product,data=example_data,xlab = "Product", ylab = "Sales",
  main = "Sales For 2001-2007")
dev.off()

