#################PROBLEM 1: Create Tables

install.packages("https://cran.r-project.org/src/contrib/Archive/RSQLite/RSQLite_0.10.0.tar.gz", repos = NULL, type = "source", dependencies = TRUE)
library("RSQLite")
conn <- dbConnect(RSQLite::SQLite(),"FinalDB_Assignment.sqlite")
#ANNUAL CROP DATA
df1<- dbExecute(conn, "CREATE TABLE CROP_DATA(
                CD_ID INTEGER NOT NULL,
                YEAR DATE NOT NULL,
                CROP_TYPE VARCHAR(20) NOT NULL,
                GEO VARCHAR(20) NOT NULL,
                SEEDED_AREA INTEGER NOT NULL,
                HARVESTED_AREA INTEGER NOT NULL,
                PRODUCTION INTEGER NOT NULL,
                AVG_YIELD INTEGER NOT NULL,
                PRIMARY KEY (CD_ID)
                )"
)
if (df1 == -1) {
  cat("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print(msg)
} else {
  cat("Table was created successfully.\n")
}

#FARM_PRICES
df2<- dbExecute(conn, "CREATE TABLE FARM_PRICES(
               CD_ID INTEGER NOT NULL,
               DATE DATE NOT NULL,
               CROP_TYPE VARCHAR(20) NOT NULL,
               GEO VARCHAR(20) NOT NULL,
               PRICE_PRERMT FLOAT(2),
               PRIMARY KEY (CD_ID)
            )"
)
if (df2 == -1) {
  cat("An error has occurred.\n")
  msg <- odbcGetErrMesg(conn)
  print(msg)
} else {
  cat("Table was created successfully. \n")
}

#DAILY_FX
df3<- dbExecute(conn, "CREATE TABLE DAILY_FX(
               DFX_ID INTEGER NOT NULL,
               DATE DATE NOT NULL,
               FXUSDCAD FLOAT(6),
               PRIMARY KEY (DFX_ID)
            )"
)
if (df3 == -1) {
  cat("An error has occurred.\n")
  msg <- odbcGetErrMesg(conn)
  print(msg)
} else {
  cat("Table was created successfully. \n")
}

#MONTHLY_FX
df4<- dbExecute(conn, "CREATE TABLE MONTHLY_FX(
               DFX_ID INTEGER NOT NULL,
               DATE DATE NOT NULL,
               FXUSDCAD FLOAT(6),
               PRIMARY KEY (DFX_ID)
            )"
)
if (df4 == -1) {
  cat("An error has occurred.\n")
  msg <- odbcGetErrMesg(conn)
  print(msg)
} else {
  cat("Table was created successfully. \n")
}

#################PROBLEM 2
#Read Datasets
crop_df <- read.csv('C:/Users/Amelia/Desktop/Annual_Crop_Data.csv',colClasses=c(YEAR="character"))
farm_df <- read.csv('C:/Users/Amelia/Desktop/Monthly_Farm_Prices.csv',colClasses=c(DATE="character"))
daily_df <- read.csv('C:/Users/Amelia/Desktop/Daily_FX.csv',colClasses=c(DATE="character"))
monthly_df <- read.csv('C:/Users/Amelia/Desktop/Monthly_FX.csv',colClasses=c(DATE="character"))

#Load Tables
dbWriteTable(conn, "CROP_DATA", crop_df, overwrite=TRUE, header = TRUE)
dbWriteTable(conn, "FARM_PRICES", farm_df, overwrite=TRUE, header = TRUE)
dbWriteTable(conn, "DAILY_FX", daily_df, overwrite=TRUE, header = TRUE)
dbWriteTable(conn, "MONTHLY_FX", monthly_df, overwrite=TRUE, header = TRUE)
dbListTables(conn)

#################SQL queries

#PROBLEM 3: How many records are in the farm prices dataset?
dbGetQuery(conn, 'SELECT COUNT(CD_ID) AS Records FROM FARM_PRICES')

#PROBLEM 4: Which provinces are included in the farm prices dataset?
dbGetQuery(conn, "SELECT DISTINCT(GEO) FROM FARM_PRICES")

#PROBLEM 5: How many hectares of Rye were harvested in Canada in 1968?
dbGetQuery(conn, "SELECT SUM(HARVESTED_AREA) AS Total_Hectares
           FROM CROP_DATA
           WHERE GEO = 'Canada' AND strftime('%Y', Year) = '1968' AND CROP_TYPE = 'Rye'")

#PROBLEM 6: Query and display the first 6 rows of the farm prices table for Rye.
dbGetQuery(conn, "SELECT *
           FROM FARM_PRICES
           WHERE CROP_TYPE = 'Rye' LIMIT 6")

#PROBLEM 7: Which provinces grew Barley?
dbGetQuery(conn, "SELECT DISTINCT(GEO)
           FROM FARM_PRICES
           WHERE CROP_TYPE='Barley'")

#PROBLEM 8: Find the first and last dates for the farm prices data.
dbGetQuery(conn, "SELECT min(DATE) FIRST_DATE, max(DATE) LAST_DATE FROM FARM_PRICES")

#PROBLEM 9: Which crops have ever reached a farm price greater than or equal to $350 per metric tonne?
dbGetQuery(conn, "SELECT DISTINCT(CROP_TYPE) FROM FARM_PRICES WHERE PRICE_PRERMT>=350")

#PROBLEM 10: Rank the crop types harvested in Saskatchewan in the year 2000 by their average yield. Which crop performed best?
dbGetQuery(conn, "SELECT CROP_TYPE, AVG_YIELD
           FROM CROP_DATA 
           WHERE GEO='Saskatchewan'AND strftime('%Y', Year) = '2000'
           ORDER BY AVG_YIELD DESC")

#PROBLEM 11: Rank the crops and geographies by their average yield (KG per hectare) since the year 2000. 
#Which crop and province had the highest average yield since the year 2000?
dbGetQuery(conn, "SELECT CROP_TYPE, GEO, AVG(AVG_YIELD) AS Avg_Field
           FROM CROP_DATA 
           WHERE strftime('%Y', YEAR) >='2000'
           GROUP BY CROP_TYPE, GEO
           ORDER BY Avg_Field DESC")

#PROBLEM 12: Use a subquery to determine how much wheat was harvested in Canada in the most recent year of the data.
dbGetQuery(conn, "SELECT MAX(YEAR) As Rec_Year, CROP_TYPE, GEO, SUM(HARVESTED_AREA) as Total_Harv 
           FROM CROP_DATA 
           WHERE GEO = 'Canada' AND CROP_TYPE = 'Wheat'
           AND YEAR=(SELECT MAX(YEAR) FROM CROP_DATA)")
#PROBLEM 13: Use an implicit inner join to calculate the monthly price per metric tonne of Canola grown in Saskatchewan in both Canadian and US dollars. 
#Display the most recent 6 months of the data.
dbGetQuery(conn, "SELECT FARM_PRICES.DATE, strftime('%m', FARM_PRICES.DATE) as Month, CROP_TYPE, GEO,
           FARM_PRICES.PRICE_PRERMT as CAD_Price, 
           (FARM_PRICES.PRICE_PRERMT / MONTHLY_FX.FXUSDCAD) as USD_Price 
           FROM FARM_PRICES, MONTHLY_FX 
           WHERE strftime('%Y', FARM_PRICES.DATE) = strftime('%Y', MONTHLY_FX.DATE) 
           AND strftime('%m', FARM_PRICES.DATE) = strftime('%m', MONTHLY_FX.DATE) 
           AND FARM_PRICES.CROP_TYPE = 'Canola' 
           AND FARM_PRICES.GEO = 'Saskatchewan' 
           ORDER BY FARM_PRICES.DATE DESC LIMIT 6")




