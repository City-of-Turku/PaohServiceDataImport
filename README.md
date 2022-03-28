# PaohServiceDataImport

# Introduction 
This repository is for fetching service data from defined sources. Service data is transformed into a rather simple document format and stored into MongoDB for service match engine to use. This container can be invoked on-demand in AKS to match the data in MongoDB and source system(s). This can be run as Azure Function in AKS with scheduled invocation.

This service can be tested by running it as localAzure function. container.

# Getting Started

Deploying locally:

You need an accessible MongoDB server and [Azure Functions Core Tools](https://www.npmjs.com/package/azure-functions-core-tools) installed on your local machine.

1. Ensure that you have a MongoDB to run on your host machine in port 27017 or use external MongoDB server, Mongo must have database `service_db` with collection `services`. Locally, you can use the predefined Mongo container from `mongo` directory in `ServiceMatchEngine` repository. If you use that, remember to fill Mongo username and password to the Mongo container `docker-compose.yaml` file of Mongo container. Then, run `docker-compose up -d` in the `mongo` directory to start the Mongo container.
2. If you run Mongo locally **without the predefined mongo container** allow access from external IPs to MongoDB by editing Mongo configuration file, by default you cannot access MongoDB from any other IP but the host
3. Add your Mongo connection info to `ServiceDataImport/ServiceDataImportFunctionApp/local.settings.json` file. This file is only used to test the function locally.  **DO NOT PUSH THIS FILE INTO REPO AFTER ADDING YOUR DETAILS**
4. Add Storage Account name and key to the same `ServiceDataImport/ServiceDataImportFunctionApp/local.settings.json` file. This is needed for function to work. You can use the Storage Account that is available in Azure test environment.
5. Run `func start` to start the function. Function should run locally as it would do in AKS.

Deploying to Azure cloud:

There is a pipeline in ServiceDataImport repository to automatically deploy changes of function into AKS testing or production when a change happens in `dev` or `main` branch.
