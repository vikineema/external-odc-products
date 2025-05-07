#!make
SHELL := /usr/bin/env bash
export GS_NO_SIGN_REQUEST := YES

setup: up init install-python-pkgs

build:
	docker compose build

## Environment setup
up: ## Bring up your Docker environment
	docker compose up -d postgres
	docker compose run checkdb
	docker compose up -d jupyter

down: ## Bring down your Docker environment
	docker compose down --remove-orphans

logs: ## View logs for all services
	docker compose logs 

init: ## Prepare the database
	docker compose exec -T jupyter datacube -v system init
	
install-python-pkgs:
	docker compose exec jupyter bash -c "cd /home/jovyan && pip install -e ."

add-products:
	docker compose exec jupyter bash -c "cd /home/jovyan/workspace && bash workflows/add_products.sh products"

mprof-plot:
	mprof plot --output=mprof_plot_$(shell date +%Y-%m-%d_%H-%M-%S).png --flame

## Jupyter service
get-jupyter-token: ## View the secret token for jupyterlab
	## Also available in .local/share/jupyter/runtime/jupyter_cookie_secret
	docker compose exec -T jupyter jupyter lab list

jupyter-shell: ## Open shell in jupyter service
	docker compose exec jupyter /bin/bash


## Postgres service
db-shell:
	PGPASSWORD=opendatacubepassword \
	pgcli -h localhost -p 5434 -U opendatacube -d postgres

## Explorer
setup-explorer: ## Setup the datacube explorer
	# Initialise and create product summaries
	docker compose up -d explorer
	docker compose exec -T explorer cubedash-gen --init --all
	# Services available on http://localhost:8080/products

## WaPOR v3
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

create-wapor-soil-moisture-stac:
	wapor-v3 create-stac-files \
	 --product-name="wapor_soil_moisture" \
	 --product-yaml="products/wapor_soil_moisture.odc-product.yaml" \
	 --stac-output-dir="s3://wapor-v3/wapor_soil_moisture/" \
	 --overwrite

index-wapor-soil-moisture:
	docker compose exec jupyter \
	s3-to-dc-v2 s3://wapor-v3/wapor_soil_moisture/**/**.json \
	--no-sign-request --update-if-exists --allow-unsafe --stac \
	wapor_soil_moisture

copy-wapor_soil_moisture:
	aws s3 cp --recursive --no-sign-request --include "*.json" --exclude "*.tif" \
	s3://wapor-v3/wapor_soil_moisture/   \
	data/wapor_soil_moisture/

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
		s3-to-dc s3://iwmi-datasets/Water_accounting_plus/Africa/Incremental_ET_M/**.json \
		--no-sign-request --update-if-exists --allow-unsafe --stac \
		iwmi_blue_et_monthly

index-iwmi_green_et_monthly:
	docker compose exec jupyter \
		s3-to-dc s3://iwmi-datasets/Water_accounting_plus/Africa/Rainfall_ET_M/**.json \
		--no-sign-request --update-if-exists --allow-unsafe --stac \
		iwmi_green_et_monthly
