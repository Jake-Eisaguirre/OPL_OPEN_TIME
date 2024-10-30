## Packages


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc)


date <- Sys.Date()


tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC for the playground database
                                  Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                  WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                  Database = "ENTERPRISE",  # Specify the database name
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication
                                  authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS") 

ot_q <- paste("select * from CT_OPEN_TIME where PAIRING_DATE > '", date,"';")

raw_ot <- dbGetQuery(db_connection, ot_q)


clean_ot <- raw_ot %>% 
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>% 
  mutate(RESERVE_POSITION = ifelse(RESERVE_POSITION %in% c("CA", "FO", "RO", "FA"),
                                   RESERVE_POSITION, 
                                   "FA"),
         BASE = if_else(BASE == "HAL", "HNL", BASE),
         RESERVE_POSITION = if_else(RESERVE_POSITION == "RO", "FO", RESERVE_POSITION)) %>% 
  group_by(PAIRING_NO, PAIRING_DATE, RESERVE_POSITION) %>% 
  filter(updated_dt == max(updated_dt),
         ORIGIN == "M",
         is.na(PAIRING_ASSIGNMENT_CODE),
         !is.na(REMOVE_CODE)) %>% 
  rename(PAIRING_POSITION = RESERVE_POSITION) %>% 
  select(CREW_INDICATOR, PAIRING_NO, PAIRING_DATE, BASE, PAIRING_POSITION)




tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC for the playground database
                                     Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                     WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                     Database = "PLAYGROUND",  # Specify the database name
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication
                                     authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS") 

dbWriteTable(db_connection_pg, "AA_OPEN_TIME", clean_ot, overwrite=T)
