library(data.table)
library(quanteda)

### General parameters used in the previous scripts, discriminated to import the relevant tables ###

texts <- "all"; sampPercent = 1         ### "all" or "sample":
                                        ### if "all", will take only the training set (80% of the total set)
                                        ### if "sample", define sampling percentage from training set
removeStopwords <- FALSE                ### if FALSE, only badwords will be removed
skipGrams <- 0                          ### integer, or vector of integers
ngrams <- 1:6                           ### integer, or vector or integers
minFreq <- 5                            ### integer with the minimum ngram frequency
prune <- FALSE; maxOccurHist <- 5       ### Should the tables be pruned?
                                        ### If so, what is the maximum number of occurrences to keep for each history?

####################################################################################################

badwords <- readLines("en_badwords.txt")
remove <- if(removeStopwords){c(badwords, stopwords())} else{badwords}

dir <- if(texts == "sample"){paste0("sample.", sprintf("%03d", sampPercent))}else if(texts == "all"){"all.train"}
wd <- paste0("C:/Users/Denny/Documents/Data Science/Data Science Specialization/Course 10 - Data Science Capstone/", dir)
setwd(wd)

fileSuffix1 <- paste0("st", as.numeric(removeStopwords))
fileSuffix2 <- paste0("sk", paste0(skipGrams, collapse = ""))
fileSuffix3 <- paste0("f", minFreq)
fileSuffix4 <- paste0("prune", as.numeric(prune) * maxOccurHist)
fileSuffix <- paste0(fileSuffix1, ".", fileSuffix2, ".", fileSuffix3, ".", fileSuffix4)

### Reading the final n-gram tables

for(n in ngrams){
    tableName <- paste0("table", n)
    temp <- fread(paste0("table", n, ".", fileSuffix), sep = "|", colClasses = c("character", "integer", "character"), key = "history")
    assign(tableName, temp)
    rm(tableName, temp)
}

### Declaration of functions

hist1F <- function(hist){
    hist1 <- unlist(strsplit(hist, split = " "))
    hist1 <- if(length(hist1) > 1){paste(hist1[2:length(hist1)], collapse = " ")}else{""}
}

predictNextWord <- function(string){
    stringT <- tokens(tolower(string), what = "word", remove_numbers = T, remove_punct = T, remove_symbols = T, remove_twitter = T, remove_hyphens = T, remove_url = T)
    stringT <- as.character(tokens_remove(stringT, remove, padding = F))
    nWordsString <- length(stringT)
    n <- ifelse(nWordsString > 5, 6, nWordsString + 1)
    hist <- paste(stringT[(nWordsString-n+2):length(stringT)], collapse = " ")
    nloop <- n
    
    while(nloop >= 1){
        cat(paste0("--Reading table ", nloop, "\n"))
        table <- get(paste0("table", nloop))
        tableFilt <- table[history == hist][order(-frequency)]
        
        if(nrow(tableFilt) > 0){
            length <- nrow(table)
            d = 0.75
            if(nloop == n){
                d = 0
            }
        
            lambda <- d/sum(tableFilt[, frequency])*nrow(tableFilt)
            
            wordsFilt <- table[word %in% tableFilt$word]$word
            
            tableFiltHead <- tableFilt[1:min(c(nrow(tableFilt), 5))
                                       , .(firstTerm = (max(frequency-d, 0))/sum(tableFilt[, frequency])
                                           , Pcont = sum(wordsFilt == word)/length)
                                       , by = word]
            
            PKN <- tableFiltHead[, .(prob = firstTerm + lambda * Pcont), by = word][word != "OTHER"][order(-prob)]
            break()
        }
        else {
            cat(paste0("  No match found. Dropping n: ", nloop, " to ", nloop-1, "\n"))
            nloop <- nloop - 1
            hist <- hist1F(hist)
        }
    }
    cat(paste0(">>> For string = \"", string, "\"\n"))
    cat(paste0(">>> ... found match from history = \"", hist, "\"\n"))
    print(head(PKN))
}

### Small test set

strings <- list(
    "1" = "The guy in front of me just bought a pound of bacon, a bouquet, and a case of",
    "2" = "You're the reason why I smile everyday. Can you follow me please? It would mean the",
    "3" = "Hey sunshine, can you follow me and make me the",
    "4" = "Very early observations on the Bills game: Offense still struggling but the",
    "5" = "Go on a romantic date at the",
    "6" = "Well I'm pretty sure my granny has some old bagpipes in her garage I'll dust them off and be on my",
    "7" = "Ohhhhh #PointBreak is on tomorrow. Love that film and haven't seen it in quite some",
    "8" = "After the ice bucket challenge Louis will push his long wet hair out of his eyes with his little",
    "9" = "Be grateful for the good times and keep the faith during the",
    "10" = "If this isn't the cutest thing you've ever seen, then you must be"
    )

### Evaluation of performance

system.time({
lapply(strings, predictNextWord)
})
