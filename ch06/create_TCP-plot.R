
png(filename="./NPtcp-plot.png", height=600, width=600, bg="white")
net_data <- read.table("NPtcp-example.data", header=T, sep=" ")
 plot(net_data, type="o", col="black")
 plot(net_data, type="o", col="black")
title(main="Netpipe TCP Plot",line = +3)
dev.off()
png(filename="./NPtcp-log-plot.png", height=600, width=600, bg="white")
net_data <- read.table("NPtcp-example.data", header=T, sep=" ")
 plot(net_data, type="o", col="black")
 plot(net_data, log="x", type="o", col="black")
title(main="Netpipe TCP Log Plot",line = +3)
dev.off()

