library(data.table)
library(sqldf)

############################## General parameters ##############################

### Used in the first script, discriminated for importing the relevant files ###
texts <- "all"; sampPercent = 1         ### "all" or "sample":
                                        ### if "all", will take only the training set (80% of the total set)
                                        ### if "sample", define sampling percentage from training set
removeStopwords <- FALSE                ### if FALSE, only badwords will be removed
skipGrams <- 0                          ### integer, or vector of integers
ngrams <- 1:6                           ### integer, or vector or integers

############################# Used in this script ##############################
minFreq <- 5                            ### integer with the minimum ngram frequency
prune <- TRUE; maxOccurHist <- 3        ### Should the tables be pruned? 
                                        ### If so, what is the maximum number of occurrences to keep for each history?

################################################################################

dir <- if(texts == "sample"){paste0("sample.", sprintf("%03d", sampPercent))}else if(texts == "all"){"all.train"}
wd <- paste0("~/", dir)
setwd(wd)

fileSuffix1 <- paste0("st", as.numeric(removeStopwords))
fileSuffix2 <- paste0("sk", paste0(skipGrams, collapse = ""))
fileSuffix <- paste0(fileSuffix1, ".", fileSuffix2)

### Declaration of functions

catT <- function(string){
    cat(paste0(string, " @ ", Sys.time(), "\n"))
}

integrateTable <- function(n, fileSuffix){
    catT(paste0("--Integrating table for ", n, "-Gram"))
    
    fileName1 <- paste0("table", n, ".", fileSuffix, ".TEMP.csv")
    fileName2 <- paste0("table", n, ".", fileSuffix, ".int.csv")
    
    if(file.exists(fileName2)){
        cat("  File exists already! Skipping table.\n")
        return(NULL)
    }
        
    con <- file(fileName1)
    
    if(file.size(fileName1)/(1024^3) > 1){           ### files larger than 1 GB won't fit in memory at once and need to be split
        catT("  Integrating part 1/2")
        table <- sqldf("SELECT ngram, SUM(frequency) AS frequency FROM con GROUP BY ngram ORDER BY frequency DESC LIMIT (SELECT COUNT(*)/2 FROM con)", dbname = "temp", file.format = list(sep = "|", colClasses = c("character", "integer"), field.types = list(ngram = "text", frequency = "integer"), filter = "C:/PROGRA~1/R/Rtools/bin/tr -d \\42"))
        catT("  Writing part 1/2 to disk")
        write.table(table, file = fileName2, sep = "|", row.names = F)    
        rm(table)
        gc()
        
        catT("  Integrating part 2/2")
        table <- sqldf("SELECT ngram, SUM(frequency) AS frequency FROM con GROUP BY ngram ORDER BY frequency DESC LIMIT (SELECT COUNT(*)/2+1 FROM con) OFFSET (SELECT (COUNT(*)/2) FROM con)", dbname = "temp", file.format = list(sep = "|", colClasses = c("character", "integer"), field.types = list(ngram = "text", frequency = "integer"), filter = "C:/PROGRA~1/R/Rtools/bin/tr -d \\42"))
        catT("  Writing part 2/2 to disk")
        write.table(table, file = fileName2, sep = "|", append = T, col.names = F, row.names = F)    
        } 
    else{
        catT("  Integrating part 1/1")
        table <- sqldf("SELECT ngram, SUM(frequency) AS frequency FROM con GROUP BY ngram ORDER BY frequency DESC", dbname = "temp", file.format = list(sep = "|", colClasses = c("character", "integer"), field.types = list(ngram = "text", frequency = "integer"), filter = "C:/PROGRA~1/R/Rtools/bin/tr -d \\42"))
        catT("  Writing part 1/1 to disk")
        write.table(table, file = fileName2, sep = "|", row.names = F)
        }

    closeAllConnections()
    rm(table)
    gc()
}

discardLowFrequency <- function(n, fileSuffix){
    catT("--Discarding low frequencies")
    
    fileName1 <- paste0("table", n, ".", fileSuffix, ".int.csv")
    fileName2 <- paste0("table", n, ".", fileSuffix, ".f", minFreq)
    
    if(file.exists(fileName2)){
        cat("  File exists already! Skipping table.\n")
        return(NULL)
    }
        
    con <- file(fileName1)

    ### The standard procedure is to discard low frequencies, meaning, all n-grams with low frequencies will be considered as non-existent. If one wishes do simply compact the table but retain the frequency counts, the two disabled lines of code below should be enabled. This approach is not recommended though, since the information concerning the identity of the n-grams, which is necessary for a correct probability calculation in the model implementation, are lost.
    
#    lowFreq <- as.numeric(sqldf(paste0("SELECT SUM(frequency) FROM con WHERE frequency < ", minFreq), dbname = "temp", file.format = list(sep = "|", colClasses = c("character", "integer"), field.types = list(ngram = "text", frequency = "integer"), filter = "C:/PROGRA~1/R/Rtools/bin/tr -d \\42")))
    table <- sqldf(paste0("SELECT ngram, frequency FROM con WHERE frequency >= ", minFreq), dbname = "temp", file.format = list(sep = "|", colClasses = c("character", "integer"), field.types = list(ngram = "text", frequency = "integer"), filter = "C:/PROGRA~1/R/Rtools/bin/tr -d \\42"))
#    table <- rbindlist(list(table, data.table(ngram = "LOWFREQS", frequency = lowFreq)))
    catT("  Writing to disk")
    write.table(table, file = fileName2, sep = "|", row.names = F)
    
    closeAllConnections()
    rm(table)
    gc()
}

splitHistoryAndWord <- function(n, fileSuffix){
    catT("--Separating history and word from N-gram")
    
    fileName1 <- paste0("table", n, ".", fileSuffix, ".f", minFreq)
    fileName2 <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".hw")
    
    if(file.exists(fileName2)){
        cat("  File exists already! Skipping table.\n")
        return(NULL)
    }
    
    table <- fread(fileName1, sep = "|", key = "ngram")
    
    if(n == 1){
        table$history <- ""
        table$word <- table$ngram
    } else{
        text <- strsplit(table$ngram, " ")
        table$history <- sapply(text, function(x) {paste(x[1:(length(x)-1)], collapse = " ")})
        table$word <- sapply(text, function(x) x[length(x)])
    }
    table[, ngram := NULL]
    
    catT("  Writing to disk")
    write.table(table, file = fileName2, sep = "|", row.names = F)
    closeAllConnections()
    rm(table)
    gc()
}

### The inclusion of a discount column was necessary when using a Katz Backoff Model. This approach was abandoned though for the (more robust, I guess) Kneser-Ney Backoff Model. The code is maintained for historical purposes.

### Calculate discount factor
### d = c* / c
### c* = (c+1) * N[c+1]/N[c]
### then d = (c+1)/c * N[c+1]/N[c]
### for c > 5, d = 1

# includeDiscountColumn <- function(n, fileSuffix, minFreq){
#     catT("--Including discount column")
#     
#     fileName1 <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".hw")
#     fileName2 <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".disc")
#     
#     if(file.exists(fileName2)){
#         cat("  File exists already! Skipping table.\n")
#         return(NULL)
#     }
#         
#     table <- fread(fileName1, sep = "|", key = "frequency", colClasses = c("character", "numeric", "character"))
#     
#     temp <- data.table(discount = rep(1, nrow(table)))
#     temp$frequency <- table$frequency
#     for(c in 5:1){
#         Nc <- nrow(temp[frequency == c])
#         Ncp1 <- nrow(temp[frequency == c+1])
#         
#         d <- (c+1)/c * Ncp1/Nc 
#         
#         temp[frequency == c, discount := d]
#     }
#     table$discount <- temp$discount
#     
#     catT("  Writing to disk")
#     write.table(table, file = fileName2, sep = "|", row.names = F)
#     closeAllConnections()
#     rm(table)
#     gc()
# }

### This approach was also abandoned since the n-grams pruned out of the table are missed when the probabilities are calculated. If one wishes to run it but perform no pruning, set prune = FALSE.

pruneTable <- function(n, minFreq, prune, maxOccurHist){
    catT("--Pruning table")
    
    # fileName1 <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".disc")
    fileName1 <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".hw")
    fileName2 <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".prune", as.numeric(prune)*maxOccurHist)
    
    if(file.exists(fileName2)){
        cat("  File exists already! Skipping table.\n")
        return(NULL)
    }
    
    # table <- fread(fileName1, sep = "|", colClasses = c("integer", "character", "character", "numeric"))[, .(history, frequency, word, discount)]
    table <- fread(fileName1, sep = "|", colClasses = c("integer", "character", "character"))[, .(history, frequency, word)]
    
    
    if(prune){
        table <- table[,.(frequency = frequency[1:max(c(maxOccurHist, .N))]
                          , word = word[1:max(c(maxOccurHist, .N))]
                          # , discount = discount[1:max(c(maxOccurHist, .N))]
                        ), 
                        by = list(history)][order(history, frequency)]
        table <- table[, .(frequency = c(frequency[.N-0:(maxOccurHist-1)], sum(frequency) - sum(frequency[.N-0:(maxOccurHist-1)]))
                           , word = c(word[.N-0:(maxOccurHist-1)], "OTHER")
                           # , discount = c(discount[.N-(0:(maxOccurHist-1))], sum(discount[(.N-maxOccurHist):1] * frequency[(.N-maxOccurHist):1])/sum(frequency[(.N-maxOccurHist):1])
                           ), 
        by = list(history)]
        table <- na.omit(table)
        table <- table[frequency != 0]
    }
    setkey(table, frequency)
    
    catT("  Writing to disk")
    write.table(table, file = fileName2, sep = "|", row.names = F)
    closeAllConnections()
    rm(table)
    gc()
}

## The TEMP files (generated in the 1st script), and the int files generated from them (this script), are too large, and will occupy memory even after the objects are deleted. so the best way to process them is individually:

integrateTable(1, fileSuffix)
.rs.restartR()
 
integrateTable(2, fileSuffix)
.rs.restartR()

integrateTable(3, fileSuffix)
.rs.restartR()

integrateTable(4, fileSuffix)
.rs.restartR()

integrateTable(5, fileSuffix)
.rs.restartR()

integrateTable(6, fileSuffix)
.rs.restartR()

## Do all the rest!

### I am choosing to build all the tables with all the combinations of parameters given in the three top for-loops. If one wishes to stick with the values discriminated in the beginning of the code, only the inner for-loop should be run.

for(minFreq in 2:5){
    for(prune in c(TRUE, FALSE)){
        for(maxOccurHist in 3:5){
            for(n in rev(ngrams)){
                catT(paste0("** PROCESSING ", n, "-GRAM **"))
                discardLowFrequency(n, fileSuffix)
                splitHistoryAndWord(n, fileSuffix)
                includeDiscountColumn(n, fileSuffix, minFreq)
                pruneTable(n, minFreq, prune, maxOccurHist)
                finalFile <- paste0("table", n, ".", fileSuffix, ".f", minFreq, ".prune", as.numeric(prune)*maxOccurHist)
                cat(paste0(">>> FINAL TABLE SIZE = ", format(file.size(finalFile)/1024/1024, digits =2), " MB\n"))
                cat("\n")
            }
        }
    }
}