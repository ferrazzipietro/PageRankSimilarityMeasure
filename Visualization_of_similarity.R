library(tidyverse)
library(plot3D)
d <- read.table(file.choose(), header=T)
d
str(d)
d$resProb <- sapply(d$resProb,function(x) gsub(",", ".",x)) %>% as.numeric
d$maxIter <- sapply(d$maxIter,function(x) gsub(",", ".",x)) %>% as.numeric
d$similarity <- sapply(d$similarity,function(x) gsub(",", ".",x)) %>% as.numeric

library(plotly)
# surface
plot_ly(x=~d$resProb, y=~d$maxIter, z=~d$similarity, type="mesh3d", 
        alpha=0.6, colors="Blues")
# points
fig <- plot_ly(d, x = ~resProb, y = ~maxIter, z = ~similarity,
               marker = list(color = ~similarity, colorscale = c('#FFE1A1', '#683531'), showscale = TRUE))
fig %>% add_markers()

# which minimum time and similarity over threshold
limit <- 0.925
d[which(d$time == min(d$time[which(d$similarity> limit)]) ),]
