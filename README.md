# Biodiversity Pipeline (Apache Beam)

This project defines a modular data pipeline built with Apache Beam. The workflow consists of four primary pipelines that process biological species data from retrieval through spatial annotation and summarisation.

---

## Project Structure

```bash
.
├── src/
│   ├── taxonomy_pipeline.py              # Retrieves and validates taxonomy of species with complete genome annotations
│   ├── occurrences_pipeline.py           # Downloads GBIF occurrences using usageKeys
│   ├── cleaning_occs_pipeline.py         # Cleans and deduplicates occurrence records
│   ├── spatial_annotation_pipeline.py    # Annotates coordinates with climate and biogeographic data
│   └── range_estimation_pipeline.py      # Estimates species' geographic range area (Extent of occurrence/Convex Hull)
│
├── utils/
│   ├── transforms.py                     # Apache Beam DoFns for data processing
│   ├── cleaning_occs.py                  # Cleaning logic for occurrence data.
│   ├── helpers.py                        # Shared utilities (schema conversion, I/O helpers, etc.)
│   └── schema_files                      # Schemas for BigQuery tables.
│
├── out/
│   ├── validated_taxonomy/               # Taxonomy validation outputs
│   ├── occurrences_raw/                  # Raw GBIF occurrence records
│   ├── occurrences_clean/                # Cleaned GBIF occurrences
│   └── spatial/                          # Annotated occurrence records and summaries
│
├── data/
│   ├── climate/                          # CHELSA raster climate data
│   ├── bioregions/                       # WWF Ecoregion shapefiles
│   └── spatial_processing/               # Natural Earth land and centroid shapefiles
│
├── docker/                               # Docker file for building the Dataflow Image.
│
├── requirements.txt
└── README.md
```

```bash

.
├── README.md
├── data
│   ├── climate/                          # CHELSA raster climate data
│   ├── bioregions/                       # WWF Ecoregion shapefiles
│   └── spatial_processing/               # Natural Earth land and centroid shapefiles
├── out/
│   ├── validated_taxonomy/               # Taxonomy validation outputs
│   ├── occurrences_raw/                  # Raw GBIF occurrence records
│   ├── occurrences_clean/                # Cleaned GBIF occurrences
│   └── spatial/                          # Annotated occurrence records and summaries
├── docker                                
│   └── Dockerfile.dataflow               # Docker file for building the Dataflow Image.
├── pyproject.toml
├── requirements.txt
└── src
    └── biodiv_pipelines
        ├── __init__.py
        ├── taxonomy_pipeline.py              # Retrieves and validates taxonomy of species with complete genome annotations
        ├── occurrences_pipeline.py           # Search and downloads a sample of GBIF occurrences using usageKeys
        ├── cleaning_occs_pipeline.py         # Cleans and deduplicates occurrence records
        ├── spatial_annotation_pipeline.py    # Annotates coordinates with climate and biogeographic data
        ├── range_estimation_pipeline.py      # Estimates species' geographic range area (Extent of occurrence/Convex Hull)
        ├── data_provenance_pipeline.py       # Collects links from data sources per species
        ├── cleaning_summary_pipeline.py 
        ├── climate_summary_pipeline.py
        ├── biogeo_summary_pipeline.py
        └── utils
            ├── __init__.py
            ├── bq_gbif_occurrences_schema.json
            ├── bq_metadata_url_schema.json
            ├── bq_range_estimates_schema.json
            ├── bq_spatial_annotation_schema.json
            ├── bq_spatial_annotation_summ_schema.json
            ├── bq_taxonomy_schema.json
            ├── cleaning_occs.py               # Cleaning logic for occurrence data.
            ├── helpers.py                     # Shared utilities (schema conversion, I/O helpers, etc.)
            └── transforms.py                  # Apache Beam DoFns for data processing

```


## Workflow Overview

1. **Taxonomy pipeline**  
   Fetch species annotations from Elasticsearch, enrich them with ENA taxonomy data, and validate scientific names via GBIF.

2. **Occurrences pipeline**  
   Download GBIF occurrence records using validated GBIF usageKeys.

3. **Cleaning pipeline**  
   Filter raw occurrence records by removing invalid coordinates, centroids, marine points, and duplicates.

4. **Spatial annotation pipeline**  
   Generate spatial buffers around cleaned occurrences and annotate them with:
   - Climate data (CHELSA rasters)
   - Biogeographic regions (WWF ecoregions)
   - Produces:
     - Per-occurrence annotations
     - Per-accession summary statistics

5. **Range estimation pipeline**  
   Calculate the geographic range (in km²) for each species by computing the convex hull of its occurrence coordinates.

---

## Local Execution

### Install the biodiv_pipelines package

```bash
pip install -e .
```

### 1. Taxonomy pipeline
Fetch genome metadata from Elasticsearch, enrich with ENA taxonomy, and validate species names using GBIF.

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host localhost \
  --user elastic \
  --password yourpassword \
  --index your_es_index \
  --output "out/validated_taxonomy/taxonomy" \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --direct_num_workers=1
```
Outputs:

- `taxonomy_validated.jsonl`
- `taxonomy_tocheck.jsonl` (**NOTE**: Contains species with FUZZY, HIGHER RANK, and SYNONYMS matches. Logic to deal with these species is still in development.) 

### 2. Occurrences pipeline

Download GBIF occurrences for validated species using their GBIF usageKey. (**NOTE**: using occurrences.search for now.)

```bash
python -m biodiv_pipelines.occurrences_pipeline \
  --validated_input out/validated_taxonomy/taxonomy_validated.jsonl \
  --output_dir out/occurrences_raw \
  --limit 100
  --direct_num_workers=1
```
Produces:

* One `occ_<sp_name>.jsonl` file per species in data/occurrences_raw/
* `summary_occ_download.jsonl` with Beam metrics: `{"SUCCESS": 124, "SKIPPED": 3, "FAILURES": 1}` 
* `dead_records.jsonl` (if any failures, e.g., GBIF API failures)

### 3. Cleaning pipeline

Filter and deduplicate raw occurrence records. Removes invalid coordinates, zero-coordinates, coordinates at the sea, and administrative centroids.

```bash
python -m biodiv_pipelines.cleaning_occs_pipeline \
  --input_glob "out/occurrences_raw/*.jsonl" \
  --output_dir "out/occurrences_clean" \
  --land_shapefile "data/spatial_processing/ne_10m_land.zip" \
  --centroid_shapefile "data/spatial_processing/ne_10m_admin_0_label_points.zip" \
  --output_consolidated "out/occurrences_clean/all_species" \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 
  ```
Outputs:

- Cleaned per-species `occ_<sp_name>.jsonl` files in `out/occurrences_clean/`

### 4. Spatial annotation pipeline

Use cleaned occurrence records to extract climate and area classification information from spatial data layers. At the moment: CHELSA climatologies and WWF Ecorregions (Dinnerstein et al. 2017).

```bash
python -m biodiv_pipelines.spatial_annotation_pipeline \
  --input_occs "out/occurrences_clean/occ_*.jsonl" \
  --climate_dir "data/climate" \
  --biogeo_vector "data/bioregions/Ecoregions2017.zip" \
  --annotated_output "out/spatial/annotated" \
  --summary_output "out/spatial/summary" 
```
Outputs:

- Annotated occurrence records in `out/spatial/annotated,jsonl`
- Summary per accession in `out/spatial/summary.jsonl`

### 5. Range estimation pipeline

```bash
python -m biodiv_pipelines.range_estimation_pipeline \
  --input_glob "out/occurrences_clean/occ_*.jsonl" \
  --bq_table your-project:your_dataset.range_estimates \
  --bq_schema "utils/bq_range_estimates_schema.json" \
  --temp_location gs://your-bucket/temp \
  --direct_num_workers=1
```
Outputs:

- A `.jsonl` file with the species and its estimated range size (i.e., area of convex hull in km<sup>2</sup>).

___
## Cloud Deployment (Google Cloud Dataflow)

Each pipeline can be deployed to Google Cloud Dataflow by:

- Replacing local paths with `gs://` paths
- Setting the `--runner` to `DataflowRunner`
- Including required GCP parameters:
  - `--project`, `--region`
  - `--temp_location`, `--staging_location`
  - `--requirements_file`
  - `--save_main_session` (for pipelines with imported modules)

### Docker Setup

To run the pipeline on Dataflow with custom dependencies or pinned versions, you need to build and push a custom Docker image using the provided Dockerfile.

#### Build and push the Docker image

1. Navigate to the root directory of the project:
```bash
   cd docker
```

2. Build the image using the provided `Dockerfile.dataflow`:

```bash
docker build --no-cache -f docker/Dockerfile.dataflow -t <region>-docker.pkg.dev/<project-id>/<repo>/biodiversity-pipeline:latest ..
```
3. Push the image to Google Artifact Registry:

```bash
docker push <region>-docker.pkg.dev/<project-id>/<repo>/biodiversity-pipeline:latest
```

### Example: Taxonomy pipeline

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host <secret> \
  --user <secret> \
  --password <secret> \
  --index <secret> \
  --size 10 \
  --pages 1 \
  --output gs://my-bucket/validated_taxonomy/taxonomy \
  --bq_table my-project:my_dataset.taxonomy \
  --bq_schema gs://my-bucket/utils/bq_taxonomy_schema.json \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```
### Example: Occurrence pipeline

```bash
python -m biodiv_pipelines.occurrences_pipeline \
  --validated_input gs://my-bucket/validated_taxonomy/test_taxonomy_validated.jsonl \
  --output_dir gs://my-bucket/test_jpng/out/occurrences_raw \
  --limit 300 \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Cleaning pipeline

```bash
python -m biodiv_pipelines.cleaning_occs_pipeline \
  --input_glob gs://my-bucket/out/occurrences_raw/occ_*.jsonl \
  --output_dir gs://my-bucket//out/occurrences_clean \
  --land_shapefile gs://my-bucket/data/spatial_processing/ne_10m_land/ne_10m_land.shp \
  --centroid_shapefile gs://my-bucket/data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --shards 5 \
  --runner DataflowRunner \
  --project my-projects \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --bq_table my-project:my_dataset.gbif_occurrences \
  --bq_schema gs://my-bucket/utils/bq_gbif_occurrences_schema.json \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Spatial annotation pipeline

```bash
 python -m biodiv_pipelines.spatial_annotation_pipeline \
  --input_occs gs://my-bucket/out/occurrences_clean/*.jsonl \
  --climate_dir gs://my-bucket/data/climate \
  --biogeo_vector gs://my-bucket/data/bioregions/Ecoregions2017/Ecoregions2017.shp \
  --annotated_output gs://my-bucket/out/spatial/spatial_annotations \
  --summary_output gs://my-bucket/out/spatial/spatial_annotations_summary \
  --bq_summary_table my-project:my_dataset.spatial_annotations \
  --bq_schema gs://my-bucket/utils/bq_spatial_annotation_summ_schema.json \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Range estimation

```bash
python -m biodiv_pipelines.range_estimation_pipeline \
  --input_glob gs://my-bucket/out/occurrences_clean/*.jsonl \
  --bq_table my-project:my_dataset.species_range_estimates \
  --bq_schema gs://my-bucket/utils/bq_range_estimates_schema.json \
  --temp_location gs://my-bucket/temp \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```