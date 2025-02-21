#!make
SHELL := /usr/bin/env bash
export GS_NO_SIGN_REQUEST := YES

build:
	docker compose build

fix-file-permissions:
	docker compose exec -it jupyter sudo chown -R 1000:100 /home/jovyan/workspace

setup-explorer:
	# Initialise and create product summaries
	docker compose exec -T explorer cubedash-gen --init --all
	docker compose exec -T explorer cubedash-run --port 8081

get-jupyter-token:
	docker compose exec -T jupyter jupyter notebook list

init: ## Prepare the database
	docker compose exec -T jupyter datacube -v system init

up: ## Bring up your Docker environment
	docker compose up -d postgres
	docker compose run checkdb
	docker compose up -d jupyter
	# make fix-file-permissions
	docker compose up -d explorer
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

create-stac-wapor_soil_moisture:
	create-stac-files \
	 --product-name="wapor_soil_moisture" \
	 --product-yaml="products/wapor_soil_moisture.odc-product.yaml" \
	 --stac-output-dir="s3://wapor-v3/wapor_soil_moisture/" \
	 --overwrite

get-storage-parameters:
	get-storage-parameters \
	--product-name="wapor_monthly_npp" \
	--output-dir="tmp/storage_parameters/" 

download-wapor-soil-moisture-cogs:
	download-wapor-v3-cogss \
	--mapset-code="L2-RSM-D" \
	--output-dir=data/wapor_soil_moisture

download-esa-worldcereal-cogs:
	download-esa-worldcereal-cogs \
	--year="2021" \
	--season="tc-wintercereals" \
	--product="wintercereals" \
	--output-dir=data/esa_worldcereal_sample/wintercereals \
	--overwrite


