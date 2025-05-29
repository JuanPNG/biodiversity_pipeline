# Biodiversity Pipeline (Apache Beam)

This project defines a modular data pipeline built with Apache Beam. The workflow consists of four primary pipelines that process biological species data from retrieval through spatial annotation and summarization.

---

## 📁 Project Structure

```bash
/src  
  ├── taxonomy_pipeline.py             # Retrieves and validates taxonomy of species with complete annotations
  ├── occurrences_pipeline.py          # Download GBIF occurrences using usageKeys
  ├── cleaning_occs_pipeline.py        # Clean and deduplicate occurrence records
  └── spatial_annotations_pipeline.py  # Annotated geographic coordinates with climate and biogeography

/utils                                 
  ├── transforms.py                    # Apache Beam DoFns
  └── helpers.py                       # Utility functions 

/out                                    
  ├── validated_taxonomy/              # Taxonomy validation output
  ├── occurrences_raw/                 # Raw GBIF occurrence data
  ├── occurrences_clean/               # Cleaned occurrences (per species)
  ├── spatial/                         # Annotated records
  └── summary/                         # Summaries for cleaned and annotated records

/data                                  # Input data and resources
  ├── climate/                         # CHELSA raster layers
  ├── bioregions/                      # WWF Ecoregions vector layers
  └── spatial_processing/              # Additional spatial layers (Land/centroid shapefiles)

requirements.txt
README.md
```
## Workflow overview

1. **Taxonomy pipeline**: Validates species names and retrieves GBIF usageKeys
2. **Occurrences pipeline**: Downloads GBIF occurrence records
3. **Cleaning pipeline**: Filters, deduplicates, and validates occurrences
4. **Spatial annotation pipeline**:

   * Generates circular buffers around occurrence coordinates
   * Annotates each record with:
     * Climate data from CHELSA rasters
     * Biogeographic regions from WWF ecoregions
   * Produces:
     * Annotated species occurrence geographic coordinates
     * Summary of annotations

---

## Local execution

Install dependencies

```bash
pip install -r requirements.txt
```
For local `.env` loading:

```bash
pip install python-dotenv
```

### 1. Taxonomy pipeline
Fetch genome metadata from Elasticsearch, enrich it via ENA, and validate species using GBIF.
```bash
python src/taxonomy_pipeline.py \
  --host localhost \
  --user elastic \
  --password yourpassword \
  --index your_es_index \
  --output out/validated_taxonomy/taxonomy2 \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --direct_num_workers=1
```

Produces:

* `out/validated_taxonomy/taxonomy_validated.jsonl`

* `out/validated_taxonomy/taxonomy_tocheck.jsonl`

### 2. Occurrences pipeline

Download GBIF occurrences for validated species using their GBIF usageKey. (NOTE: using occurrences.search for now.)

```bash
python src/occurrences_pipeline.py \
  --validated_input out/validated_taxonomy/taxonomy_validated.jsonl \
  --output_dir out/occurrences_raw \
  --limit 100
  --direct_num_workers=1
```
Produces:

* One `.jsonl` file per species in data/occurrences_raw/
* A `summary.jsonl` with Beam-tracked metrics: `{"SUCCESS": 124, "SKIPPED": 3, "FAILURES": 1}` (NOTE: Fix currently in same directory.)
* Dead records (e.g., GBIF API failures) go to dead_records.jsonl only if any failures occurred. (NOTE: Fix currently in same directory.)

### 3. Cleaning pipeline

Eliminate duplicates and filters raw occurrences following best practices to work with occurrence data: Duplicates, zero-coordinates, out-of-bound coordinates, points at the sea, and country centroids.

```bash
python src/cleaning_occs_pipeline.py \
  --input_glob "out/occurrences_raw/*.jsonl" \
  --output_dir "out/occurrences_clean" \
  --land_shapefile "data/spatial_processing/ne_10m_land.zip" \
  --centroid_shapefile "data/spatial_processing/ne_10m_admin_0_label_points.zip" \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --direct_num_workers=1
  ```
Produces:

* One `.jsonl` file per species in data/occurrences_clean/ and cleaning summary. 

### 4. Spatial annotation pipeline

Use cleaned occurrence records to extract climate and area classification information from spatial data layers. At the moment: CHELSA climatologies and WWF Ecorregions (Dinnerstein et al. 2017).

```bash
python src/spatial_annotations_pipeline.py \
  --input_occs "out/occurrences_clean/*.jsonl" \
  --climate_dir data/climate \
  --biogeo_vector data/bioregions/Ecoregions2017.zip \
  --annotated_output out/spatial/annotated \
  --climate_summary_output out/spatial/climate_summary \
  --biogeo_summary_output out/spatial/biogeo_summary \
  --direct_num_workers=1
```

---

### Cloud Deployment

Each pipeline can be deployed to Google Cloud by updating output/input paths to `gs://` and setting the appropriate options.

Example with the **Taxonomy pipeline**:

```bash 
python src/taxonomy_pipeline.py \
  --host <secret> \
  --user <secret> \
  --password <secret> \
  --index species_index \
  --output gs://my-bucket/validates_taxonomy/taxonomy \
  --runner DataflowRunner \
  --project my-gcp-project \
  --region europe-west1 \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --max_num_workers=4 \
  --autoscaling_algorithm=NONE \
  --requirements_file requirements.txt
```
Other pipelines follow a similar pattern — ensure:

* Input/output paths use GCS
* You define `--runner`, `--project`, and staging/temp paths
* Include `--save_main_session` if required

Example with cleaning pipeline

```bash
python src/cleaning_occs_pipeline.py \
  --input_glob "out/occurrences_raw/*.jsonl" \
  --output_dir "out/occurrences_clean" \
  --land_shapefile "data/spatial_processing/ne_10m_land.zip" \
  --centroid_shapefile "data/spatial_processing/ne_10m_admin_0_label_points.zip" \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --bq_table your-project:your_dataset.gbif_occurrences \
  --bq_schema utils/bq_gbif_occurrences_schema.json \
  --temp_location gs://your-bucket/temp \
  --runner DataflowRunner \
  --project your-project \
  --region europe-west1 \
  --requirements_file requirements.txt \
  --save_main_session
```


______
OLD
____

Example with the **Occurrences pipeline**:

```bash
python src/occurrences_pipeline.py \
  --validated_input gs://your-bucket/validated/species_validated.jsonl \
  --output_dir gs://your-bucket/output/occurrences \
  --limit 150 \
  --runner DataflowRunner \
  --project your-project-id \
  --region europe-west1 \
  --temp_location gs://your-bucket/temp \
  --staging_location gs://your-bucket/staging \
  --requirements_file requirements.txt \
  --save_main_session
```

Example with the **Cleaning Pipeline**:

Requirements

* Natural Earth shapefiles: landmass and admin-0 label points
* Cleaned schema JSON if loading to BigQuery: utils/bq_occurrence_schema.json

#### Local Execution (No BigQuery)

```
python src/cleaning_occs_pipeline.py \
  --input_glob "out/occurrences_raw/*.jsonl" \
  --output_dir "out/occurrences_clean" \
  --land_shapefile "data/spatial_processing/ne_10m_land.zip" \
  --centroid_shapefile "data/spatial_processing/ne_10m_admin_0_label_points.zip" \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --direct_num_workers=4
 ```

Produces:

* One .jsonl file per species in out/occurrences_clean/

#### GCP Execution (With BigQuery)

```
python src/cleaning_occs_pipeline.py \
  --input_glob "gs://your-bucket/occurrences_raw/*.jsonl" \
  --output_dir "gs://your-bucket/cleaned_occurrences" \
  --land_shapefile "gs://your-bucket/shapes/ne_10m_land.zip" \
  --centroid_shapefile "gs://your-bucket/shapes/ne_10m_admin_0_label_points.zip" \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --bq_table your-project:your_dataset.cleaned_occurrences \
  --bq_schema utils/bq_occurrence_schema.json \
  --temp_location gs://your-bucket/temp \
  --staging_location gs://your-bucket/staging \
  --runner DataflowRunner \
  --project your-project \
  --region europe-west1 \
  --requirements_file requirements.txt \
  --save_main_session
```
