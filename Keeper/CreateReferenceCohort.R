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
referenceCohortTable <- "reference_cohort_optum_extended_dod_v3787"
referenceCohortId <- 2
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
  phenotypeName = "Acute Myocardial Infarction",
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
                            phenotypeName = "Acute Myocardial Infarction",
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
# Optum Dod:   Cohort size is 16,472,835 persons, 1,677,093 with the diagnosis, and 14,795,742 with a combination of other markers.

readr::write_csv(specConcepts, "Keeper/specConceptsOptumDod.csv")


# Run Keeper on the sensitive cohort -----------------------------------------------------------------------------------
keeperHsc <- generateKeeper(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 2,
  sampleSize = 10000,
  phenotypeName = "Acute Myocardial Infarction",
  keeperConceptSets = conceptSets,
  removePii = FALSE
)
saveRDS(keeperHsc, file.path(folder, "keeperAmiHsc.rds"))


# Run LLM adjudication on highly-sensitive cohort ----------------------------------------------------------------------
keeperHsc <- readRDS(file.path(folder, "keeperAmiHsc.rds"))

library(ellmer)
client <- chat_azure_openai(
  endpoint = gsub("/openai/deployments.*", "", keyring::key_get("genai_o3_endpoint")),
  api_version = "2024-12-01-preview",
  model = "o3",
  credentials = function() keyring::key_get("genai_api_gpt4_key")
)
promptSettings <- createPromptSettings()
llmReviewsHsc <- reviewCases(keeper = keeperHsc,
                             settings = promptSettings,
                             client = client,
                             cacheFolder =  file.path(folder, "cacheamiHsc"))
saveRDS(llmReviewsHsc, file.path(folder, "llmReviewsAmiHsc.rds"))

# Counts per case status and certainty:
llmReviewsHsc |> 
  group_by(isCase, certainty) |>
  count()
# isCase certainty     n
# no     high       9066
# no     low          41
# yes    high        774
# yes    low         119

# Count cases with unclear index date:
llmReviewsHsc |> 
  filter(isCase == "yes") |>
  mutate(unknownIndexDay = indexDay == -9999) |>
  group_by(unknownIndexDay) |>
  count()
# unknownIndexDay     n
# FALSE             792
# TRUE              101

# Count by case status and whether the person had an AMI diagnosis:
amiConcepts <- c(312327,319039,434376,436706,438170,438438,438447,441579,444406,604425,604427,604429,604430,761736,
                 1199824,3654465,3654466,3654467,3655133,3661502,3661503,3661504,3661520,3661524,3661547,3661641,
                 3661642,3661643,3661644,3661645,3661646,4051874,4108669,4119456,4119457,4119943,4119944,4119945,
                 4119946,4119947,4119948,4121464,4121465,4121466,4124684,4124685,4126801,4145721,4178129,4243372,
                 4267568,4270024,4275436,4296653,4303359,4324413,35610091,35610093,35611570,35611571,37160791,37163019,
                 37163020,37163022,37163023,37163084,37163289,37169262,37172157,37172159,37172160,37172162,37172163,
                 43020460,44782712,44782769,45766075,45766076,45766115,45766116,45766150,45766151,45771322,46270158,
                 46270159,46270160,46270161,46270162,46270163,46270164,46273495,46274044)
dxGeneratedIds <- keeperHsc |>
  filter(conceptId %in% amiConcepts) |>
  pull(generatedId) |>
  unique()
llmReviewsHsc |>
  mutate(hasDx = generatedId %in% dxGeneratedIds) |>
  group_by(isCase, hasDx) |>
  count()
# isCase hasDx     n
# no     FALSE  8796
# no     TRUE    311
# yes    FALSE   179
# yes    TRUE    714


# Upload reference cohort to server ------------------------------------------------------------------------------------
llmReviewsHsc <- readRDS(file.path(folder, "llmReviewsAmiHsc.rds"))

uploadReferenceCohort(
  connectionDetails = connectionDetails,
  referenceCohortDatabaseSchema = referenceCohortDatabaseSchema,
  referenceCohortTableNames = createReferenceCohortTableNames(referenceCohortTable),
  referenceCohortDefinitionId = referenceCohortId,
  createReferenceCohortTables = FALSE,
  reviews = llmReviewsHsc
)

# (Some code to verify upload)
connection <- DatabaseConnector::connect(connectionDetails)
metadata <- DatabaseConnector::renderTranslateQuerySql(
  connection= connection,
  sql = "SELECT * FROM @schema.@table_metadata;",
  schema = referenceCohortDatabaseSchema,
  table = referenceCohortTable
)
metadata
DatabaseConnector::renderTranslateQuerySql(
  connection= connection,
  sql = "SELECT cohort_definition_id, is_case, COUNT(*) AS profile_count FROM @schema.@table GROUP BY cohort_definition_id, is_case;",
  schema = referenceCohortDatabaseSchema,
  table = referenceCohortTable
)
DatabaseConnector::disconnect(connection)


# Compute cohort operating characteristics -----------------------------------------------------------------------------
metrics <- computeCohortOperatingCharacteristics(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortDefinitionId = 1,
  referenceCohortDatabaseSchema = referenceCohortDatabaseSchema,
  referenceCohortTableNames = createReferenceCohortTableNames(referenceCohortTable),
  referenceCohortDefinitionId = 2
)
metrics |>
  mutate(
    sensitivity = sprintf("%0.2f (%0.2f-%0.2f)", sensitivity, sensitivityLb, sensitivityUb),
    specificity = sprintf("%0.2f (%0.2f-%0.2f)", specificity, specificityLb, specificityUb),
    ppv = sprintf("%0.2f (%0.2f-%0.2f)", ppv, ppvLb, ppvUb)
    ) |>
  select(tp, fp, tn, fn, sensitivity, specificity, ppv)
#  tp    fp    tn    fn sensitivity      specificity      ppv             
# 675   149  8942    90 0.88 (0.86-0.90) 0.98 (0.98-0.99) 0.82 (0.79-0.84)