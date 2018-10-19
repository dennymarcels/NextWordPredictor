library(quanteda)
library(data.table)

### General parameters ###

texts <- "all"; sampPercent = 1         ### "all" or "sample":
                                        ### if "all", will take only the training set (80% of the total set)
                                        ### if "sample", define sampling percentage from training set
removeStopwords <- TRUE                 ### if FALSE, only badwords will be removed
skipGrams <- 0                          ### integer, or vector of integers
ngrams <- 1:6                           ### integer, or vector or integers

#########################

### Preprocessing files ###
#
# con <- file("en_US.blogs.txt", open = "rb")
# blogs <- readLines(con, encoding = "UTF-8", skipNul = T)
# close(con)
# blogs <- iconv(blogs, from = "UTF-8", to = "ASCII", sub = "")
# 
# con <- file("en_US.news.txt", open = "rb")
# news <- readLines(con, encoding = "UTF-8", skipNul = T)
# close(con)
# news <- iconv(news, from = "UTF-8", to = "ASCII", sub = "")
# 
# con <- file("en_US.twitter.txt", open = "rb")
# twitter <- readLines(con, encoding = "UTF-8", skipNul = T)
# close(con)
# twitter <- iconv(twitter, from = "UTF-8", to = "ASCII", sub = "")
# 
# all <- c(blogs, news, twitter)
# rm(blogs, news, twitter)
# set.seed(1)
# all <- sample(all)
# 
# file.create("all.conv.txt")
# con <- file("all.conv.txt", open = "wb")
# writeLines(all, con)
# close(con)
# 
# allTrain <- all[1:ceiling(length(all)*0.8)]
# file.create("all.train.conv.txt")
# con <- file("all.train.conv.txt", open = "wb")
# writeLines(allTrain, con)
# close(con)
# rm(allTrain)
# 
# allTest <- all[(ceiling(length(all)*0.8)+1):length(all)]
# file.create("all.test.conv.txt")
# con <- file("all.test.conv.txt", open = "wb")
# writeLines(allTest, con)
# close(con)
# rm(allTest)

### Loading files and defining global variables ###
quanteda_options(threads = 2)

badwords <- readLines("en_badwords.txt")
remove <- if(removeStopwords){c(badwords, stopwords())} else{badwords}

con <- 
    if(texts == "all"){
        filePrefix <- "all.train"
        file("all.train.conv.txt", open = "rb") 
    } else{if(texts == "sample"){
        filePrefix <- paste0("sample.", sprintf("%03d", sampPercent))
        sampleFile <- paste0(filePrefix, ".conv.txt")
        if(file.exists(sampleFile)){
            file(sampleFile, open = "rb")
        } else{
            file("all.train.conv.txt", open = "rb")
        }
    }
    }
        
raw <- readLines(con, encoding = "UTF-8")
close(con)

if(texts == "sample"){
    if(!file.exists(sampleFile)){
        raw <- raw[1:ceiling(length(raw)*sampPercent/100)]
        file.create(sampleFile)
        con <- file(sampleFile, open = "wb")
        writeLines(raw, con)
        close(con)
    }
}

length <- length(raw)

##################################################

### Declaration of functions ###

dfmPar <- function(corpus, ngrams, remove, skipGrams){
    tokens <- tokens(corpus, what = "word", remove_numbers = T, remove_punct = T, remove_symbols = T, remove_twitter = T, remove_hyphens = T, remove_url = T, verbose = F)
    tokens <- tokens_remove(tokens, pattern = remove, padding = F, verbose = T)
    dfm(tokens, tolower = T, ngrams = ngrams, skip = skipGrams, concatenator = " ", verbose = T)
}

dfmToTable <- function(dfm){
    table <- topfeatures(dfm, n = dim(dfm)[2], scheme = "count")
    table <- data.table(ngram = names(table), frequency = table, stringsAsFactors = F)
    table
}

tableToFile <- function(table, removeStopwords, skipGrams){
    fileSuffix1 <- paste0("st", as.numeric(removeStopwords))
    fileSuffix2 <- paste0("sk", paste0(skipGrams, collapse = ""))
    fileSuffix <- paste0(fileSuffix1, ".", fileSuffix2)
    dir <- paste0(filePrefix)
    if(!dir.exists(dir)){dir.create(dir)}
    
    fileName <- paste0(dir, "/table", n, ".", fileSuffix, ".TEMP.csv")
    if(chunk == 1){
        write.table(table, file = fileName, sep = "|", row.names = F)
    }
    else {
        write.table(table, file = fileName, sep = "|", append = T, col.names = F, row.names = F)
    }
}

processChunk <- function(raw, chunk){
    closeAllConnections()
    print(paste0("Running ", n, "-gram: chunk ", chunk, " of ", chunks))
    group <- groups == chunk
    raw0 <- raw[group]
    rm(group)
    
    print("--Building corpus")
    corpus <- corpus(raw0)
    rm(raw0)
    corpus <- corpus_reshape(corpus, to = "sentences")
    
    print("--Building DFM")
    dfm <- dfmPar(corpus, n, remove, skipGrams)
    rm(corpus)
    
    print("--Building table")
    table <- dfmToTable(dfm)
    rm(dfm)
    
    print("--Writing table to file")
    tableToFile(table, removeStopwords, skipGrams)
    rm(table)
    
    closeAllConnections()
}

#############################

### Loop to generate n-gram tables ###

for(n in 6){
    chunks <- ceiling(length/3415743 * length(skipGrams) * 3 * n)
    chunk_length <- length/chunks
    groups <- ceiling(seq_along(raw)/chunk_length)
    
    print(paste0("!! STARTING ", n, "-GRAM CONSTRUCTION !!"))
    chunk <- 1
    while(chunk <= chunks){
        processChunk(raw, chunk)
        chunk <- chunk + 1
    }
    rm(chunk)
}

######################################