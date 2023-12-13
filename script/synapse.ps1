az login --service-principal -u f979e510-233c-4df4-bcb6-de42db1d374b -p=l3o8Q~h3-T1DRSPgVprVl1fHJnS4EEuAFKexgc12 --tenant e339bd4b-2e3b-4035-a452-2112d502f2ff
az account set --subscription f161f906-7364-4800-a570-108277edf40f
az synapse pipeline create-run --name Pl_airflow_synapse_test --workspace-name synw-dataanalytics-dataplatform-dev-westeurope-nr
