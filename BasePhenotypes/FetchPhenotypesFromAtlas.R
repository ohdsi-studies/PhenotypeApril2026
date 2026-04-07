library(ROhdsiWebApi)
library(dplyr)

baseUrl <- "http://atlas-demo.ohdsi.org:80/WebAPI"
folder <- "BasePhenotypes"

cohortIds <- c(1796887,
               1796888,
               1796889,
               1796890,
               1796891,
               1796892,
               1796893,
               1796894,
               1796895)

cohortId = cohortIds[1]
cohortRef <- list()
for (cohortId in cohortIds) {
  cohortDefinition <- getCohortDefinition(cohortId = cohortId, baseUrl = baseUrl)
  
  json <- RJSONIO::toJSON(cohortDefinition$expression, pretty = TRUE)
  fileName <- file.path(folder, sprintf("cohort%s.json", cohortId))
  writeLines(json, fileName)
  
  cohortRef[[length(cohortRef) + 1]] <- tibble(
    cohortId = cohortId,
    cohortName = cohortDefinition$name,
    jsonFileName = fileName
  )
}
cohortRef <- bind_rows(cohortRef)
readr::write_csv(cohortRef, file.path(folder, "cohortRef.csv"))
