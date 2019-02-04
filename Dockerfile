FROM cityofcapetown/datascience:rstudio_shiny
#FROM rocker/tidyverse:3.5.1

# Installing R packages
RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
RUN R -e "install.packages('Rblpapi')"
RUN R -e "install.packages('here')"
RUN R -e "install.packages('doParallel')"
RUN R -e "install.packages('xts')"
RUN R -e "install.packages('dygraphs')"
RUN R -e "install.packages('pbo')"

# For compatibility with documentation
ENV NEWUSER=rstudio
ENV PASSWD=complicatedpassword

# Clone in the repo
RUN useradd rstudio
RUN mkdir /home/rstudio/ && \
    cd /home/rstudio/ && \
    git clone --single-branch --branch lag-correction https://github.com/riazarbi/equity_analysis.git && \
    chmod -R 777 equity_analysis
