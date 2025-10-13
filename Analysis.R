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
set.seed(20)

# Spark
Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64")
system("java -version")

conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9
sc <- spark_connect(master = "local", version="3.3.2", spark_home = "~/spark/spark-3.3.2-bin-hadoop3", config = conf)


# Upliv podataka
iowa_liquor_sales <- spark_read_csv(sc,
                             name = "iowa_liquor_sales",
                             path = "./data/iowa_liquor_sales.csv",
                             header = TRUE,
                             infer_schema = TRUE)

# Preview the first 5 rows
head(iowa_liquor_sales, 5)
sdf_schema(iowa_liquor_sales)

# Spark 
spark_disconnect(sc)
