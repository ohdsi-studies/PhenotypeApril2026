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

connection <- connect(connectionDetails)
allMetrics <- list()
for (i in seq_len(nrow(cohortRef))) {
  message("Evaluating ", cohortRef$cohortName[i])
  metrics <- computeCohortOperatingCharacteristics(
    connection = connection,
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
disconnect(connection)
allMetrics <- bind_rows(allMetrics)
readr::write_csv(allMetrics, file.path(folder, "cohortMetrics.csv"))

allMetrics |>
  mutate(
    sensitivity = sprintf("%0.2f (%0.2f-%0.2f)", sensitivity, sensitivityLb, sensitivityUb),
    specificity = sprintf("%0.2f (%0.2f-%0.2f)", specificity, specificityLb, specificityUb),
    ppv = sprintf("%0.2f (%0.2f-%0.2f)", ppv, ppvLb, ppvUb)
  ) |>
  select(tp, fp, tn, fn, sensitivity, specificity, ppv)
#  tp    fp    tn    fn sensitivity      specificity      ppv             
# 511   200  8897   264 0.66 (0.62-0.69) 0.98 (0.97-0.98) 0.72 (0.68-0.75)
# 511   200  8896   263 0.66 (0.63-0.69) 0.98 (0.97-0.98) 0.72 (0.68-0.75)
# 486   120  8976   301 0.62 (0.58-0.65) 0.99 (0.98-0.99) 0.80 (0.77-0.83)
#   0     0  9107   792 0.00 (0.00-0.00) 1.00 (1.00-1.00) NA (NA-NA)      
# 201     9  9098   591 0.25 (0.22-0.29) 1.00 (1.00-1.00) 0.96 (0.92-0.98)
# 367    45  9061   424 0.46 (0.43-0.50) 1.00 (0.99-1.00) 0.89 (0.86-0.92)
#  57    24  9038   707 0.07 (0.06-0.10) 1.00 (1.00-1.00) 0.70 (0.59-0.80)
# 479   139  8913   292 0.62 (0.59-0.66) 0.98 (0.98-0.99) 0.78 (0.74-0.81)
# 360   112  8957   412 0.47 (0.43-0.50) 0.99 (0.99-0.99) 0.76 (0.72-0.80)
# 503   217  8835   255 0.66 (0.63-0.70) 0.98 (0.97-0.98) 0.70 (0.66-0.73)
