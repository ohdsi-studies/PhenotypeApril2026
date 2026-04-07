library(CohortGenerator)
library(dplyr)
library(CirceR)
library(DatabaseConnector)
library(Keeper)

connectionDetails <- createConnectionDetails(
  dbms = "spark",
  connectionString = keyring::key_get("databricksConnectionString"),
  user = "token",
  password = keyring::key_get("databricksToken")
)
cdmDatabaseSchema <- "optum_extended_dod.cdm_optum_extended_dod_v3787"
cohortDatabaseSchema <- "scratch.scratch_mschuemi"
cohortTable <- "phenotype_april_cohort"
referenceCohortDatabaseSchema <- "scratch.scratch_all"
referenceCohortTable <- "reference_cohort_optum_extended_dod_v3787"
referenceCohortId <- 2
options(sqlRenderTempEmulationSchema = "scratch.scratch_mschuemi")

folder <- "BasePhenotypes"

# Generate cohorts -----------------------------------------------------------------------------------------------------
cohortRef <- readr::read_csv(file.path(folder, "cohortRef.csv"), show_col_types = FALSE)

cohortRef$json <- sapply(cohortRef$jsonFileName, function(x) paste(readLines(x), collapse = "\n"))
cohortRef$sql <- ""
for (i in seq_len(nrow(cohortRef))) {
  options <- createGenerateOptions(
    cohortId = cohortRef$cohortId[i],
    generateStats = TRUE
    
  )
  cohortRef$sql[i] <- buildCohortQuery(cohortRef$json[i], options)
}

connection <- connect(connectionDetails)
generateCohortSet(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(cohortTable),
  cohortDefinitionSet = cohortRef
)
counts <- getCohortCounts(
  connection = connection,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortIds = cohortRef$cohortId
)
disconnect(connection)

counts <- cohortRef |>
  select("cohortId", "cohortName") |>
  inner_join(counts, by = join_by(cohortId))
readr::write_csv(counts, file.path(folder, "cohortCounts.csv"))

# Evaluate using Keeper reference cohort -------------------------------------------------------------------------------
cohortRef <- readr::read_csv(file.path(folder, "cohortRef.csv"), show_col_types = FALSE)

allMetrics <- list()
for (i in seq_len(nrow(cohortRef))) {
  metrics <- computeCohortOperatingCharacteristics(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortDefinitionId = cohortRef$cohortId[i],
    referenceCohortDatabaseSchema = referenceCohortDatabaseSchema,
    referenceCohortTableNames = createReferenceCohortTableNames(referenceCohortTable),
    referenceCohortDefinitionId = referenceCohortId
  )
  metrics <- metrics |>
    mutate(cohortId = cohortRef$cohortId[i],
           cohortName = cohortRef$cohortName[i]) |>
    relocate(cohortId, cohortName)
  allMetrics[[i]] <- metrics
}
allMetrics <- bind_rows(allMetrics)
readr::write_csv(allMetrics, file.path(folder, "cohortMetrics.csv"))
