library(Keeper)
library(dplyr)
library(ellmer)
library(DatabaseConnector)

client <- chat_azure_openai(
  endpoint = gsub("/openai/deployments.*", "", keyring::key_get("genai_o3_endpoint")),
  api_version = "2024-12-01-preview",
  model = "o3",
  credentials = function() keyring::key_get("genai_api_gpt4_key")
)

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
referenceCohortTable <- "phenotype_april_reference_cohort_optum_extended_dod_v3787"
options(sqlRenderTempEmulationSchema = "scratch.scratch_mschuemi")

folder <- "e:/PhenotypeApril2026"

# Create concept sets --------------------------------------------------------------------------------------------------
conceptSets <- generateKeeperConceptSets(
  phenotype = "Acute Myocardial Infarction",
  client = client,
  vocabConnectionDetails = connectionDetails,
  vocabDatabaseSchema = cdmDatabaseSchema
)
readr::write_csv(conceptSets, "Keeper/amiConceptSets.csv")

# Create simple cohort -------------------------------------------------------------------------------------------------
library(Capr)

amiCs <- cs(
  c(descendants(4329847), descendants(exclude(314666))),
  name = "Acute Myocardial Infarction"
)

inpatientOrEr <- cs(descendants(262, 9201),
         name = "Inpatient or inpatient ER visit")

amiCohort <- cohort(
  entry = entry(
    conditionOccurrence(
      conceptSet = amiCs,
      nestedWithAll(
        atLeast(
          1, 
          aperture = duringInterval(eventStarts(-Inf, 0), eventEnds(0, Inf)),
          query = visit(conceptSet = inpatientOrEr)
        )
      )
    ),
    primaryCriteriaLimit = "First"
  )
)

# Note: this will automatically assign cohort ID 1:
cohortSet <- makeCohortSet(amiCohort)

library(CohortGenerator)
connection <- connect(connectionDetails)
createCohortTables(
  connection = connection,
  cohortTableNames = getCohortTableNames(cohortTable),
  cohortDatabaseSchema = cohortDatabaseSchema
)
CohortGenerator::generateCohortSet(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(cohortTable),
  cohortDefinitionSet = cohortSet
)
disconnect(connection)


# Run Keeper on the cohort ---------------------------------------------------------------------------------------------
conceptSets <- readr::read_csv("Keeper/amiConceptSets.csv")

keeper <- generateKeeper(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 1,
  sampleSize = 100,
  phenotypeName = "Atrial fibrillation",
  keeperConceptSets = conceptSets,
  removePii = TRUE
)

saveRDS(keeper, file.path(folder, "amiKeeper.rds"))


# Run LLM adjudication -------------------------------------------------------------------------------------------------
keeper <- readRDS(file.path(folder, "amiKeeper.rds"))

library(ellmer)
client <- chat_azure_openai(
  endpoint = gsub("/openai/deployments.*", "", keyring::key_get("genai_o3_endpoint")),
  api_version = "2024-12-01-preview",
  model = "o3",
  credentials = function() keyring::key_get("genai_api_gpt4_key")
)
promptSettings <- createPromptSettings()
llmResponses <- reviewCases(keeper = keeper,
                            settings = promptSettings,
                            phenotypeName = "Atrial Fibrillation",
                            client = client,
                            cacheFolder = file.path(folder, "cacheami"))
llmResponses |>
  group_by(isCase) |>
  summarise(n())
saveRDS(llmResponses, file.path(folder, "llmReviewsami.rds"))

# Create highly sensitive cohort ---------------------------------------------------------------------------------------
conceptSets <- readr::read_csv("Keeper/amiConceptSets.csv")

specConcepts <- createSensitiveCohort(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 2,
  createCohortTable = FALSE,
  keeperConceptSets = conceptSets
)
# Optum Dod:  Cohort size is 26,840,645 persons, 2,990,763 with the diagnosis, and 23,849,882 with a combination of other markers.

readr::write_csv(conceptSets, "Keeper/specConceptsOptumDod.csv")

# Run Keeper on the sensitive cohort -----------------------------------------------------------------------------------
keeperHsc <- generateKeeper(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 2,
  sampleSize = 10000,
  phenotypeName = "Atrial Fibrillation",
  keeperConceptSets = conceptSets,
  removePii = FALSE
)
saveRDS(keeperHsc, file.path(folder, "keeperamiHsc.rds"))


# Run LLM adjudication on highly-sensitive cohort ----------------------------------------------------------------------
keeperHsc <- readRDS(file.path(folder, "keeperamiHsc.rds"))

library(ellmer)
client <- chat_azure_openai(
  endpoint = gsub("/openai/deployments.*", "", keyring::key_get("genai_o3_endpoint")),
  api_version = "2024-12-01-preview",
  model = "o3",
  credentials = function() keyring::key_get("genai_api_gpt4_key")
)
promptSettings <- createPromptSettings()
llmResponsesHsc <- reviewCases(keeper = keeperHsc,
                               settings = promptSettings,
                               client = client,
                               cacheFolder =  file.path(folder, "cacheamiHsc"))
saveRDS(llmResponsesHsc, file.path(folder, "llmReviewsamiHsc.rds"))


# Upload reference cohort to server ------------------------------------------------------------------------------------
llmResponsesHsc <- readRDS(file.path(folder, "llmReviewsamiHsc.rds"))

uploadReferenceCohort(
  connectionDetails = connectionDetails,
  referenceCohortDatabaseSchema = referenceCohortDatabaseSchema,
  referenceCohortTableNames = createReferenceCohortTableNames(referenceCohortTable),
  referenceCohortDefinitionId = 1,
  createReferenceCohortTables = TRUE,
  reviews = llmResponsesHsc
)

# Compute cohort operating characteristics -----------------------------------------------------------------------------
metrics <- computeCohortOperatingCharacteristics(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 1,
  referenceCohortDatabaseSchema = referenceCohortDatabaseSchema,
  referenceCohortTableNames = createReferenceCohortTableNames(referenceCohortTable),
  referenceCohortDefinitionId = 1
)

