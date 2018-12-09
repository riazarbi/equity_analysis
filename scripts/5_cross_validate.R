# Clear environment
rm(list=ls())
source("R/set_paths.R")

# Move Rmd to results
file.copy(from="scripts/reporting/cross_validate.Rmd", to="results/cross_validate.Rmd", 
          overwrite = TRUE, recursive = FALSE, 
          copy.mode = TRUE)

# Knit Rmd
rmarkdown::render("results/cross_validate.Rmd")