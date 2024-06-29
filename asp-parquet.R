#rm(list = ls())
#libraries and global parameters----
library(dplyr)
library(arrow)
options(arrow.use_threads = TRUE)
arrow:::set_cpu_count(10)

n = stringr::str_locate(getwd(),"Documents")[2]
outputdir = paste0(stringr::str_sub(getwd(),1,n),"/data")
datatype = c("apt", "cie", "lsn")
subdir = paste0(outputdir,"/asp-", datatype)

#functions----
write_allcsv_from_dir_to_singleparquetfile = function(directory)
{
  f = list.files(path = directory, pattern = "csv\\b", all.files = TRUE, full.names = TRUE, recursive = TRUE, ignore.case=T) #select files with names ending in "csv"
  df = NULL
  for (datapath in f){
    tmp = read.csv2(datapath)
    df = rbind(df,tmp)
  }#row bind all files
  
  datapath = paste0(directory,".parquet")
  write_parquet(
    x = df,
    sink = datapath
  )#write to WD parquet file with name of directory
}

#write csv files from dir to single parquet file----
for (i in 1:3){
  write_allcsv_from_dir_to_singleparquetfile(subdir[i])
}
