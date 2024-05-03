#!/bin/bash
# Execute from repo root dir:
# $ export $(grep -v '^#' .env.localhost.prod | xargs)
# $ ./util/mongodump-nmdc.sh
#
# Note: consider getting a known backup of the production database, e.g.
# $ scp -r dtn01.nersc.gov:/global/cfs/cdirs/m3408/nmdc-mongodumps/dump_nmdc-prod_2024-03-11_20-12-02 .
#
mongodump -h $MONGO_HOST -u $MONGO_USERNAME -p $MONGO_PASSWORD --authenticationDatabase=admin \
    -d $MONGO_DBNAME \
    --gzip -o $HOME/nmdcdb-mongodump/nmdcdb/$(date +"%Y-%m-%dT%H")/ \
    --excludeCollectionsWithPrefix="_runtime" \
    --excludeCollectionsWithPrefix="_tmp" \
    --excludeCollectionsWithPrefix="fs." \
    --excludeCollectionsWithPrefix="ids_" \
    --excludeCollectionsWithPrefix="txn_log" \
    --excludeCollectionsWithPrefix="functional_annotation_agg"