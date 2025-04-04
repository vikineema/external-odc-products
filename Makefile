#!make
SHELL := /usr/bin/env bash
export GS_NO_SIGN_REQUEST := YES
include .env
export

build:
	docker compose build

fix-file-permissions:
	docker compose exec -it jupyter sudo chown -R 1000:100 /home/jovyan/workspace

setup-explorer:
	# Initialise and create product summaries
	docker compose up -d explorer
	docker compose exec -T explorer cubedash-gen --init --all
	# Do not run this below it messes up the configuration
	# http://localhost:8080/products
	# docker compose exec -T explorer cubedash-run #--port 8081

get-jupyter-token:
	docker compose exec -T jupyter jupyter notebook list

init: ## Prepare the database
	docker compose exec -T jupyter datacube -v system init

up: ## Bring up your Docker environment
	docker compose up -d postgres
	docker compose run checkdb
	docker compose up -d jupyter
	# make fix-file-permissions
	# docker compose up -d explorer
	make init
	make add-products

down:
	docker compose down --remove-orphans

logs:
	docker compose logs

shell:
	docker compose exec jupyter bash -c "cd /home/jovyan/workspace && exec bash"

add-products:
	docker compose exec jupyter bash -c "cd /home/jovyan/workspace && bash workflows/add_products.sh products"

## WaPOR v3

# Download and crop WaPOR version 3 GeoTIFFs
download-wapor-monthly-npp-cogs:
	wapor-v3 download-cogs \
	--mapset-code="L2-NPP-M" \
	--output-dir=data/wapor_monthly_npp/ \
	--no-overwrite

download-wapor-soil-moisture-cogs:
	wapor-v3 download-cogs \
	--mapset-code="L2-RSM-D" \
	--output-dir=data/wapor_soil_moisture/ \
	--no-overwrite

# Create stac files for the WaPOR version 3 COGS
create-wapor-soil-moisture-stac:
	wapor-v3 create-stac-files \
	 --product-name="wapor_soil_moisture" \
	 --product-yaml="products/wapor_soil_moisture.odc-product.yaml" \
	 --stac-output-dir="s3://wapor-v3/wapor_soil_moisture/" \
	 --overwrite

# Index stac files
index-wapor-soil-moisture:
	docker compose exec jupyter \
	s3-to-dc-v2 s3://wapor-v3/wapor_soil_moisture/**/**.json \
	--no-sign-request --update-if-exists --allow-unsafe --stac \
	wapor_soil_moisture

#  Copy stac files from s3
copy-wapor_soil_moisture:
	aws s3 cp --recursive --no-sign-request --include "*.json" --exclude "*.tif" \
	s3://wapor-v3/wapor_soil_moisture/   \
	data/wapor_soil_moisture/

## ESA WorldCereal
download-esa-worldcereal-cogs:
	esa-wordlcereal download-cogs \
	--year="2021" \
	--season="tc-wintercereals" \
	--product="wintercereals" \
	--output-dir=data/esa_worldcereal_sample/  \
	--no-overwrite

get-storage-parameters-esa_worldcereal_wintercereals-1:
	get-storage-parameters \
	--product-name=esa_worldcereal_wintercereals_classification \
	--geotiffs-dir="s3://deafrica-data-dev-af/esa_worldcereal_sample/wintercereals/tc-wintercereals/" \
	--pattern=''.*classification\\.tif$' \
	--output-dir="tmp/storage_parameters"

get-storage-parameters-esa_worldcereal_wintercereals-2:
	get-storage-parameters \
	--product-name=esa_worldcereal_wintercereals_confidence \
	--geotiffs-dir="s3://deafrica-data-dev-af/esa_worldcereal_sample/wintercereals/tc-wintercereals/" \
	--pattern=''.*confidence\\.tif$' \
	--output-dir="tmp/storage_parameters"

create-esa-wordlcereal-stac:
	mprof run --include-children \
    esa-wordlcereal create-stac-files \
		--product-name="esa_worldcereal_wintercereals" \
		--product-yaml="products/esa_worldcereal_wintercereals.odc-product.yaml" \
		--geotiffs-dir="s3://deafrica-data-dev-af/esa_worldcereal_sample/wintercereals/tc-wintercereals/" \
		--stac-output-dir="data/esa_worldcereal_sample/" \
		--overwrite
	make plot

# Index stac files
index-esa-wordlcereal:
	docker compose exec jupyter \
	s3-to-dc-v2 s3://deafrica-data-dev-af/esa_worldcereal_sample/wintercereals/tc-wintercereals/**/**/**.stac-item.json \
	--no-sign-request --update-if-exists --allow-unsafe --stac \
	esa_worldcereal_wintercereals

mprof-plot:
	mprof plot --output=mprof_plot_$(shell date +%Y-%m-%d_%H-%M-%S).png --flame

## Sentinel-3
get-storage-parameters-s3_olci_l2_lfr:
	get-storage-parameters \
	--product-name=s3_olci_l2_lfr_GIFAPAR \
	--geotiffs-dir=s3://deafrica-sentinel-3-dev/Sentinel-3/OLCI/OL_2_LFR/2025/01/ \
	--pattern=''.*GIFAPAR\\.tif$' \
	--output-dir=tmp/storage_parameters 

index-s3_olci_l2_lfr:
	docker compose exec jupyter \
	s3-to-dc-v2 s3://deafrica-sentinel-3-dev/Sentinel-3/OLCI/OL_2_LFR/2025/**/**.json \
	--no-sign-request --update-if-exists --allow-unsafe --stac \
	s3_olci_l2_lfr

copy-s3_olci_l2_lfr:
	aws s3 cp --recursive --no-sign-request --include "*.json" --exclude "*.tif" \
	s3://deafrica-sentinel-3-dev/Sentinel-3/OLCI/OL_2_LFR/2025/   \
	data/s3_olci_l2_lfr/

delete-product:
	export PRODUCT_NAME=s3_olci_l2_lfr
	cd workflows/odc-product-delete && ./delete_product.sh


## IMWI ODR
copy-iwmi_blue_et_monthly:
	aws s3 cp --recursive --no-sign-request --include "*.json" --exclude "*.tif" \
	s3://iwmi-datasets/Water_accounting_plus/Africa/Incremental_ET_M/   \
	data/iwmi_blue_et_monthly/

index-iwmi_blue_et_monthly:
	docker compose exec jupyter \
		s3-to-dc-v2 s3://iwmi-datasets/Water_accounting_plus/Africa/Incremental_ET_M/**.json \
		--no-sign-request --update-if-exists --allow-unsafe --stac \
		iwmi_blue_et_monthly

index-iwmi_green_et_monthly:
	docker compose exec jupyter \
		s3-to-dc-v2 s3://iwmi-datasets/Water_accounting_plus/Africa/Rainfall_ET_M/**.json \
		--no-sign-request --update-if-exists --allow-unsafe --stac \
		iwmi_green_et_monthly
