# Define paths and load libraries
rm(list=ls())
source("R/set_paths.R")
source("results/parameters.R")
library(pbo)
# Time the script
allbegin <- Sys.time()

######################################################
# read in returns
# remove date column because it is not required by pbo package
daily_returns <- read_feather(file.path(results_directory, "daily_returns.feather"))
total_returns <- read_feather(file.path(results_directory, "total_returns.feather"))

# define sharpe ratio calulation function
# copied exactly from the pbo package vignette
# but replaced their rf wih ours
sharpe <- function(x,rf=daily_risk_free_rate) {
  sr <- apply(x,2,function(col) {
    er = col - rf
    return(mean(er)/sd(er))
  })
  return(sr)
}

library(ggplot2)
library(reshape2)

df_melt = melt(total_returns, id.vars = 'date')
ggplot(df_melt, aes(x = date, y = value)) + 
  geom_line() + 
  facet_wrap(~ variable, scales = 'free_y', ncol = 1)

ts.plot(total_returns %>% select(-date), gpars= list(col=rainbow(10)))

my_pbo <- pbo(portfolio_returns,s=8,f=sharpe,threshold=0)

summary(my_pbo) #closer to 1 pbo is more overfit

require(lattice)
require(latticeExtra)
require(grid)
histogram(my_pbo, type='density')
xyplot(my_pbo,plotType="degradation")
xyplot(my_pbo,plotType="dominance",increment=0.001)
xyplot(my_pbo,plotType="pairs")
xyplot(my_pbo,plotType="ranks",ylim=c(0,20))
dotplot(my_pbo)
