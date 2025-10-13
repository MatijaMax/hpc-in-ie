# Packets
install.packages("ggpubr")
install.packages("sparklyr")
install.packages("caret")
install.packages("readr")


packages <- c("ggpubr","sparklyr","caret","readr")
installed <- packages %in% rownames(installed.packages())
names(installed) <- packages
installed

# Libs
library(ggplot2)
library(readr)
library(dplyr)
library(magrittr)
library(lubridate)
library(ggpubr)
library(sparklyr)
library(caret)
library(tibble)
set.seed(20)

# Spark
Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64")
system("java -version")

conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9
sc <- spark_connect(master = "local", version="3.3.2", spark_home = "~/spark/spark-3.3.2-bin-hadoop3", config = conf)

### PRIPREMA PODATAKA [3p] ###

# Upliv podataka
iowa_liquor_sales <- spark_read_csv(sc,
                             name = "iowa_liquor_sales",
                             path = "./data/iowa_liquor_sales.csv",
                             header = TRUE,
                             infer_schema = TRUE)

# Prvih 5 redova
head(iowa_liquor_sales, 5)
sdf_schema(iowa_liquor_sales)

# 1. red
first_row <- iowa_liquor_sales %>% head(1) %>% collect()
first_row_tibble <- tibble(
  Column = names(first_row),
  Value  = as.character(unlist(first_row))
)
print(first_row_tibble, n = Inf)

# Clean
iowa_clean <- iowa_liquor_sales %>%
  select(Date, Store_Number, Store_Name, City, County,
         Category, Category_Name, Vendor_Name, Bottle_Volume_ml,
         State_Bottle_Cost, State_Bottle_Retail,
         Bottles_Sold, Sale_Dollars, Volume_Sold_Liters, Volume_Sold_Gallons)

head(iowa_clean, 5)

iowa_clean <- iowa_clean %>%
  mutate(Date =to_date(Date, "MM/dd/yyyy"),
         Bottle_Volume_ml  = as.numeric(Bottle_Volume_ml),
         State_Bottle_Cost = as.numeric(State_Bottle_Cost),
         State_Bottle_Retail = as.numeric(State_Bottle_Retail),
         Sale_Dollars = as.numeric(Sale_Dollars),
         Volume_Sold_Liters = as.numeric(Volume_Sold_Liters),
         Volume_Sold_Gallons = as.numeric(Volume_Sold_Gallons))

head(iowa_clean, 5)
sdf_schema(iowa_clean)

iowa_clean %>% summarise(n_rows = n()) %>% collect()

missing_counts <- iowa_clean %>%
  summarise(
    across(everything(), ~sum(if_else(is.na(.), 1L, 0L)))
  ) %>%
  collect()

missing_counts_t <- t(missing_counts)
print(missing_counts_t)

### PRELIMINARNA ANALIZA PODATAKA [3p] ###

sdf_schema(iowa_clean)


# Spark 
spark_disconnect(sc)
