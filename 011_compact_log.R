# LOG COMPACTION 

# for two dates, if the value, source, ticker and metric are the same, drop the later date.
# what about timestamps? there will be duplicates every time we pull. 
# We want the latest timestamp, to know that what the last refresh date was.
# Is the latest refresh the most important value? Yes, I think it is.

# So first filter for timestamp, then for date on the timestamp filtered data.

# Create a compaction function
# SEND TO UTILS.R

library(tidyverse)

compact_log <- function(dataframe) {

# Drop old timestamps of duplicate data
timestamp_filtered <- dataframe %>% 
  mutate(key=paste(date, value, source, sep = "|")) %>%
  arrange(desc(key)) %>%
  filter(key != lag(key, default="0")) %>% 
  select(-key)

# Drop row where the data doesn't change from day to day
compact_data <- timestamp_filtered %>% 
  mutate(key=paste(value, source, sep = "|")) %>%
  arrange(date) %>%
  filter(key != lag(key, default="0")) %>% 
  select(-key)

return(compact_data)

}

