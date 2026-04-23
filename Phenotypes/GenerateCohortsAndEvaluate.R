# This requires you have the Keeper referene cohort instantiated
#
# How to use:
# 1. Ensure the right packages are installed (see commented-out code below).
# 2. Ensure the connection details etc. below are correct for you.
# 3. Add your cohort JSONs as separate files to the Phenotypes folder
# 4. Add rows to the cohortRef.csv for your cohorts.
# 5. Rerun the code below.
# 6. Read the operating characteristics in cohortMetrics.csv


# Install required packages:
# options(install.packages.compile.from.source = "never")
# install.packages("curl")
# install.packages("remotes")
# install.packages("dplyr")
# install.packages("keyring")
# install.packages("readr")
# install.packages("DatabaseConnector")
# install.packages("SqlRender")
# remotes::install_github("ohdsi/CohortGenerator", upgrade = "always")
# remotes::install_github("ohdsi/CirceR", upgrade = "always")
# remotes::install_github("ohdsi/Keeper", ref = "f60ed03ebbcfaf6aae53fc04d6f00c9429e5f818", upgrade = "always")

library(CohortGenerator)
library(dplyr)
library(CirceR)
library(DatabaseConnector)
library(Keeper)
library(readr)

connectionDetails <- createConnectionDetails(
  dbms = "spark",
  connectionString = keyring::key_get("databricksConnectionString"),
  user = "token",
  password = keyring::key_get("databricksToken")
)
cdmDatabaseSchema <- "optum_extended_dod.cdm_optum_extended_dod_v3787"
cohortDatabaseSchema <- "scratch.scratch_all"
cohortTable <- "phenotype_april_cohort_optum_extended_dod_v3787"
referenceCohortDatabaseSchema <- "scratch.scratch_all"
referenceCohortTable <- "reference_cohort_optum_extended_dod_v3787"
referenceCohortId <- 2
options(sqlRenderTempEmulationSchema = "scratch.scratch_all")

folder <- "Phenotypes"


# Generate cohorts -----------------------------------------------------------------------------------------------------
cohortRef <- read_csv(file.path(folder, "cohortRef.csv"), show_col_types = FALSE)

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
createCohortTables(
  connection = connection,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(cohortTable),
  incremental = TRUE
)
generateCohortSet(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(cohortTable),
  cohortDefinitionSet = cohortRef,
  incremental = TRUE
)
counts <- getCohortCounts(
  connection = connection,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable
)

disconnect(connection)

counts <- cohortRef |>
  select("cohortId", "cohortName") |>
  inner_join(counts, by = join_by(cohortId))
write_csv(counts, file.path(folder, "cohortCounts.csv"))


# Evaluate using Keeper reference cohort -------------------------------------------------------------------------------
cohortRef <- read_csv(file.path(folder, "cohortRef.csv"), show_col_types = FALSE)

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
    relocate("cohortId", "cohortName")
  allMetrics[[i]] <- metrics
}
disconnect(connection)
allMetrics <- bind_rows(allMetrics)
write_csv(allMetrics, file.path(folder, "cohortMetrics.csv"))

