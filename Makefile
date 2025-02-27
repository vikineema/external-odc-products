#!make
SHELL := /usr/bin/env bash
export GS_NO_SIGN_REQUEST := YES

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

get-storage-parameters:
	get-storage-parameters \
	--geotiffs-dir=data/esa_worldcereal_sample/wintercereals \
	--output-dir="tmp/storage_parameters/" 


## WaPOR v3

# Download and crop WaPOR version 3 GeoTIFFs
download-wapor-monthly-npp-cogs:
	download-wapor-v3-cogs \
	--mapset-code="L2-NPP-M" \
	--output-dir=data/wapor_monthly_npp/ \
	--no-overwrite

download-wapor-soil-moisture-cogs:
	download-wapor-v3-cogs \
	--mapset-code="L2-RSM-D" \
	--output-dir=data/wapor_soil_moisture/ \
	--no-overwrite

# Create stac files for the WaPOR version 3 COGS
create-wapor-soil-moisture-stac:
	create-stac-files \
	 --product-name="wapor_soil_moisture" \
	 --product-yaml="products/wapor_soil_moisture.odc-product.yaml" \
	 --stac-output-dir="s3://wapor-v3/wapor_soil_moisture/" \
	 --overwrite

# Index stac files
index-wapor-soil-moisture:
	docker compose exec jupyter \
	s3-to-dc s3://wapor-v3/wapor_soil_moisture/**/**.json \
	--no-sign-request --update-if-exists --allow-unsafe --stac \
	wapor_soil_moisture


## ESA WorldCereal

download-esa-worldcereal-cogs:
	download-esa-worldcereal-cogs \
	--year="2021" \
	--season="tc-wintercereals" \
	--product="wintercereals" \
	--output-dir=data/esa_worldcereal_sample/  \
	--no-overwrite

create-esa-wordlcereal-stac:
	create-esa-wordlcereal-stac \
	 --product-name="esa_worldcereal_wintercereals" \
	 --product-yaml="esa_worldcereal_wintercereals.odc-product.yaml" \
	 --geotiffs-dir="s3://deafrica-data-dev-af/esa_worldcereal_sample/wintercereals/tc-wintercereals/" \
	 --stac-output-dir="data/esa_worldcereal_sample/eodatasets3_0_30_7" \
	 --no-overwrite