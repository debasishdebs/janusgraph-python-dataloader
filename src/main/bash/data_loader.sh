#!/usr/bin/env bash

while getopts ":m:s:c:l:" opt; do
  case ${opt} in
    m) data_mapper="$OPTARG"
    ;;
    s) schema_file="$OPTARG"
    ;;
    c) config_file="$OPTARG"
    ;;
    l) data_file="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG. Usage is bash data_loader_poc.sh -m [Data_Mapper_File]
    -s [Schema_File] -c [JanusGraph_Config_File] -l [Data_File]" >&2
    ;;
  esac
done

printf "Argument Data Mapper is %s\n" "${data_mapper}"
printf "Argument Data loading file (abs path) %s\n" "${data_file}"
printf "Argument Config file for JanusGraph (abs path) %s\n" "${config_file}"
printf "Argument Schema file (abs path) %s\n" "${schema_file}"

data_loader_file="../python/DataLoader.py"

printf "Starting Data Loading  by running ${data_loader_file}"
python ${data_loader_file} --data_mapper_file ${data_mapper} --schema_file ${schema_file} --data_file ${data_file} --config_file ${config_file}
printf "Finished loading all data for Source from ${data_file} Logs file"
