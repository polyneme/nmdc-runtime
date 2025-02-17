import re
import sqlite3
from typing import Dict, Optional, Union

import pandas as pd
import requests
import requests_cache

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator
from nmdc_runtime.site.util import get_basename
from nmdc_runtime.site.translation.neon_utils import (
    _get_value_or_none,
    _create_controlled_identified_term_value,
    _create_controlled_term_value,
    _create_geolocation_value,
    _create_quantity_value,
    _create_timestamp_value,
    _create_text_value,
)


SURFACE_WATER_BROAD_SCALE_MAPPINGS = {
    "lake": {"term_id": "ENVO:01000252", "term_name": "freshwater lake biome"},
    "river": {"term_id": "ENVO:01000253", "term_name": "freshwater river biome"},
    "stream": {"term_id": "ENVO:03605008", "term_name": "freshwater stream biome"},
}

SURFACE_WATER_LOCAL_SCALE_MAPPINGS = {
    "lake": {
        "buoy.c0": {"term_id": "ENVO:01001191", "term_name": "water surface"},
        "buoy.c1": {"term_id": "ENVO:00002131", "term_name": "epilimnion"},
        "buoy.c2": {"term_id": "ENVO:00002269", "term_name": "thermocline"},
        "buoy.c3": {"term_id": "ENVO:00002130", "term_name": "hypolimnion"},
        "littoral": {
            "term_id": "ENVO:01000409",
            "term_name": "freshwater littoral zone",
        },
        "inflow": {"term_id": "ENVO:00000476", "term_name": "lake inlet"},
    },
    "river": {"term_id": "ENVO:01000297", "term_name": "freshwater river"},
    "stream": {"term_id": "ENVO:03605007", "term_name": "freshwater stream"},
}

SURFACE_WATER_MEDIUM_MAPPINGS = {
    "lake": {
        "term_id": "ENVO:04000007",
        "term_name": "lake water",
    },
    "river": {"term_id": "ENVO:01000599", "term_name": "river water"},
    "stream": {"term_id": "ENVO:03605006", "term_name": "stream water"},
}


class NeonSurfaceWaterDataTranslator(Translator):
    def __init__(
        self,
        surface_water_data: dict,
        site_code_mapping: dict,
        neon_envo_mappings_file: pd.DataFrame,
        neon_raw_data_file_mappings_file: pd.DataFrame,
        neon_nmdc_instrument_map_df: pd.DataFrame = pd.DataFrame(),
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.conn = sqlite3.connect("neon.db")
        requests_cache.install_cache("neon_api_cache")

        neon_amb_data_tables = (
            "mms_swMetagenomeSequencing",
            "mms_swMetagenomeDnaExtraction",
            "mms_swRawDataFiles",
            "amc_fieldGenetic",
            "amc_fieldSuperParent",
        )

        if all(k in surface_water_data for k in neon_amb_data_tables):
            surface_water_data["mms_swMetagenomeSequencing"].to_sql(
                "mms_swMetagenomeSequencing",
                self.conn,
                if_exists="replace",
                index=False,
            )
            surface_water_data["mms_swMetagenomeDnaExtraction"].to_sql(
                "mms_swMetagenomeDnaExtraction",
                self.conn,
                if_exists="replace",
                index=False,
            )
            surface_water_data["mms_swRawDataFiles"].to_sql(
                "mms_swRawDataFiles", self.conn, if_exists="replace", index=False
            )
            surface_water_data["amc_fieldGenetic"].to_sql(
                "amc_fieldGenetic", self.conn, if_exists="replace", index=False
            )
            surface_water_data["amc_fieldSuperParent"].to_sql(
                "amc_fieldSuperParent", self.conn, if_exists="replace", index=False
            )
        else:
            raise ValueError(
                f"You are missing one of the aquatic benthic microbiome tables: {neon_amb_data_tables}"
            )

        neon_envo_mappings_file.to_sql(
            "neonEnvoTerms", self.conn, if_exists="replace", index=False
        )

        self.neon_raw_data_file_mappings_df = surface_water_data["mms_swRawDataFiles"]

        self.site_code_mapping = site_code_mapping

        self.neon_nmdc_instrument_map_df = neon_nmdc_instrument_map_df

    def _translate_biosample(
        self, neon_id: str, nmdc_id: str, biosample_row: pd.DataFrame
    ) -> nmdc.Biosample:
        def map_local_scale(
            aquatic_site_type: str, named_location: Optional[str] = None
        ) -> Dict[str, str]:
            if aquatic_site_type == "lake":
                for key in SURFACE_WATER_LOCAL_SCALE_MAPPINGS.get(
                    aquatic_site_type, {}
                ):
                    if key in named_location:
                        return SURFACE_WATER_LOCAL_SCALE_MAPPINGS[aquatic_site_type][
                            key
                        ]
            elif aquatic_site_type == "river" or aquatic_site_type == "stream":
                return SURFACE_WATER_LOCAL_SCALE_MAPPINGS.get(aquatic_site_type, {})
            return {}

        depth = None
        minimum_depth = biosample_row["lakeSampleDepth1"].values[0]
        maximum_depth = biosample_row["lakeSampleDepth2"].values[0]

        if not pd.isna(minimum_depth):
            if not pd.isna(maximum_depth):
                depth = nmdc.QuantityValue(
                    has_minimum_numeric_value=nmdc.Float(minimum_depth),
                    has_maximum_numeric_value=nmdc.Float(maximum_depth),
                    has_unit="m",
                    type="nmdc:QuantityValue",
                )
            else:
                depth = nmdc.QuantityValue(
                    has_numeric_value=nmdc.Float(minimum_depth),
                    has_unit="m",
                    type="nmdc:QuantityValue",
                )

        return nmdc.Biosample(
            id=nmdc_id,
            env_broad_scale=_create_controlled_identified_term_value(
                SURFACE_WATER_BROAD_SCALE_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_id"),
                SURFACE_WATER_BROAD_SCALE_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_name"),
            ),
            env_local_scale=_create_controlled_identified_term_value(
                map_local_scale(
                    biosample_row["aquaticSiteType"].values[0],
                    biosample_row["namedLocation"].values[0],
                ).get("term_id"),
                map_local_scale(
                    biosample_row["aquaticSiteType"].values[0],
                    biosample_row["namedLocation"].values[0],
                ).get("term_name"),
            ),
            env_medium=_create_controlled_identified_term_value(
                SURFACE_WATER_MEDIUM_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_id"),
                SURFACE_WATER_MEDIUM_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_name"),
            ),
            name=neon_id,
            lat_lon=_create_geolocation_value(
                biosample_row["decimalLatitude"].values[0],
                biosample_row["decimalLongitude"].values[0],
            ),
            elev=nmdc.Float(biosample_row["elevation"].values[0]),
            collection_date=_create_timestamp_value(
                biosample_row["seqCollectDate"].values[0]
            ),
            geo_loc_name=_create_text_value(
                self.site_code_mapping[biosample_row["siteID"].values[0]]
                if biosample_row["siteID"].values[0]
                else None
            ),
            samp_collec_device=biosample_row["samplerType"].values[0],
            diss_oxygen=_create_quantity_value(
                biosample_row["dissolvedOxygen"].values[0], "mg/L"
            ),
            conduc=_create_quantity_value(
                biosample_row["specificConductance"].values[0], "uS/cm"
            ),
            temp=_create_quantity_value(biosample_row["waterTemp"].values[0], "Cel"),
            type="nmdc:Biosample",
            analysis_type="metagenomics",
            biosample_categories="NEON",
            depth=depth,
            samp_size=_create_quantity_value(
                biosample_row["geneticFilteredSampleVolume"].values[0], "mL"
            ),
            env_package=nmdc.TextValue(has_raw_value="water", type="nmdc:TextValue"),
            associated_studies=["nmdc:sty-11-hht5sb92"],
        )

    def _translate_extraction_process(
        self,
        extraction_id: str,
        extraction_input: str,
        processed_sample_id: str,
        extraction_row: pd.DataFrame,
    ) -> nmdc.Extraction:
        """
        Create an nmdc Extraction process, which is a process to model the DNA extraction in
        a metagenome sequencing experiment. The input to an Extraction process is the
        output from a Pooling process.

        :param extraction_id: Minted id for Extraction process.
        :param extraction_input: Input to an Extraction process is the output from a Pooling process.
        :param processed_sample_id: Output of Extraction process is a ProcessedSample.
        :param extraction_row: DataFrame with Extraction process metadata.
        :return: Extraction process object.
        """
        processing_institution = None
        laboratory_name = _get_value_or_none(extraction_row, "laboratoryName")
        if laboratory_name is not None:
            if re.search("Battelle", laboratory_name, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", laboratory_name, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.Extraction(
            id=extraction_id,
            has_input=extraction_input,
            has_output=processed_sample_id,
            start_date=_get_value_or_none(extraction_row, "extrCollectDate"),
            end_date=_get_value_or_none(extraction_row, "extrProcessedDate"),
            input_mass=_create_quantity_value(
                _get_value_or_none(extraction_row, "sampleMass"), "g"
            ),
            qc_status=nmdc.StatusEnum(
                _get_value_or_none(extraction_row, "extrQaqcStatus")
            ),
            processing_institution=processing_institution,
            type="nmdc:Extraction",
        )

    def _translate_library_preparation(
        self,
        library_preparation_id: str,
        library_preparation_input: str,
        processed_sample_id: str,
        library_preparation_row: pd.DataFrame,
    ):
        """
        Create LibraryPreparation process object. The input to LibraryPreparation process
        is the output ProcessedSample from an Extraction process. The output of LibraryPreparation
        process is fed as input to an NucleotideSequencing object.

        :param library_preparation_id: Minted id for LibraryPreparation process.
        :param library_preparation_input: Input to LibraryPreparation process is output from
        Extraction process.
        :param processed_sample_id: Minted ProcessedSample id which is output of LibraryPreparation
        is also input to NucleotideSequencing.
        :param library_preparation_row: Metadata required to populate LibraryPreparation.
        :return: Object that using LibraryPreparation process model.
        """
        processing_institution = None
        laboratory_name = _get_value_or_none(library_preparation_row, "laboratoryName")
        if laboratory_name is not None:
            if re.search("Battelle", laboratory_name, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", laboratory_name, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.LibraryPreparation(
            id=library_preparation_id,
            has_input=library_preparation_input,
            has_output=processed_sample_id,
            start_date=_get_value_or_none(library_preparation_row, "seqCollectDate"),
            end_date=_get_value_or_none(library_preparation_row, "seqProcessedDate"),
            processing_institution=processing_institution,
            type="nmdc:LibraryPreparation",
        )

    def _get_instrument_id(self, instrument_model: Union[str | None]) -> str:
        if not instrument_model:
            raise ValueError(
                f"instrument_model '{instrument_model}' could not be found in the NEON-NMDC instrument mapping TSV file."
            )

        df = self.neon_nmdc_instrument_map_df
        matching_row = df[
            df["NEON sequencingMethod"].str.contains(instrument_model, case=False)
        ]

        if not matching_row.empty:
            nmdc_instrument_id = matching_row["NMDC instrument_set id"].values[0]
            return nmdc_instrument_id

    def _translate_nucleotide_sequencing(
        self,
        nucleotide_sequencing_id: str,
        processed_sample_id: str,
        raw_data_file_data: str,
        nucleotide_sequencing_row: pd.DataFrame,
    ):
        """Create nmdc NucleotideSequencing object. This class typically models the run of a
        Bioinformatics workflow on sequence data from a biosample. The input to an NucleotideSequencing
        process is the output from a LibraryPreparation process, and the output of NucleotideSequencing
        is a DataObject which has the FASTQ sequence file URLs embedded in them.

        :param nucleotide_sequencing_id: Minted id for an NucleotideSequencing process.
        :param processed_sample_id: ProcessedSample that is the output of LibraryPreparation.
        :param raw_data_file_data: R1/R2 DataObjects which have links to workflow processed output
        files embedded in them.
        :param nucleotide_sequencing_row: DataFrame with metadata for an NucleotideSequencing workflow
        process/run.
        :return: NucleotideSequencing object that models a Bioinformatics workflow process/run.
        """
        processing_institution = None
        sequencing_facility = _get_value_or_none(
            nucleotide_sequencing_row, "sequencingFacilityID"
        )
        if sequencing_facility is not None:
            if re.search("Battelle", sequencing_facility, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", sequencing_facility, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.NucleotideSequencing(
            id=nucleotide_sequencing_id,
            has_input=processed_sample_id,
            has_output=raw_data_file_data,
            processing_institution=processing_institution,
            ncbi_project_name=_get_value_or_none(
                nucleotide_sequencing_row, "ncbiProjectID"
            ),
            instrument_used=self._get_instrument_id(
                _get_value_or_none(nucleotide_sequencing_row, "instrument_model")
            ),
            name=f"Surface water microbial communities - {_get_value_or_none(nucleotide_sequencing_row, 'dnaSampleID')}",
            type="nmdc:NucleotideSequencing",
            associated_studies=["nmdc:sty-11-hht5sb92"],
            analyte_category="metagenome",
        )

    def _translate_processed_sample(
        self, processed_sample_id: str, sample_id: str
    ) -> nmdc.ProcessedSample:
        """
        Create an nmdc ProcessedSample. ProcessedSample is typically the output of a PlannedProcess
        like Pooling, Extraction, LibraryPreparation, etc. We are using this to create a
        reference for the nmdc minted ProcessedSample ids in `processed_sample_set`. We are
        associating the minted ids with the name of the sample it is coming from which can be
        a value from either the `genomicsSampleID` column or from the `dnaSampleID` column.

        :param processed_sample_id: NMDC minted ProcessedSampleID.
        :param sample_id: Value from `genomicsSampleID` or `dnaSampleID` column.
        :return: ProcessedSample objects to be stored in `processed_sample_set`.
        """
        return nmdc.ProcessedSample(
            id=processed_sample_id, name=sample_id, type="nmdc:ProcessedSample"
        )

    def _translate_data_object(
        self, do_id: str, url: str, do_type: str, manifest_id: str
    ) -> nmdc.DataObject:
        """Create nmdc DataObject which is the output of a NucleotideSequencing process. This
        object mainly contains information about the sequencing file that was generated as
        the result of running a Bioinformatics workflow on a certain ProcessedSample, which
        is the result of a LibraryPreparation process.

        :param do_id: NMDC minted DataObject id.
        :param url: URL of zipped FASTQ file on NEON file server. Retrieved from file provided
        by Hugh Cross at NEON.
        :param do_type: Indicate whether it is FASTQ for Read 1 or Read 2 (paired end sequencing).
        :param checksum: Checksum value for FASTQ in zip file, once again provided by Hugh Cross
        at NEON.
        :return: DataObject with all the sequencing file metadata.
        """
        file_name = get_basename(url)
        basename = file_name.split(".", 1)[0]

        return nmdc.DataObject(
            id=do_id,
            name=file_name,
            url=url,
            description=f"sequencing results for {basename}",
            type="nmdc:DataObject",
            data_object_type=do_type,
            in_manifest=manifest_id,
        )

    def _translate_manifest(self, manifest_id: str) -> nmdc.Manifest:
        return nmdc.Manifest(
            id=manifest_id,
            manifest_category=nmdc.ManifestCategoryEnum.poolable_replicates,
            type="nmdc:Manifest",
        )

    def get_database(self):
        database = nmdc.Database()

        query = """
                SELECT
                    merged.laboratoryName,
                    merged.sequencingFacilityID,
                    merged.extrProcessedDate,
                    merged.seqProcessedDate,
                    merged.dnaSampleID,
                    merged.internalLabID,
                    merged.instrument_model,
                    merged.sequencingMethod,
                    merged.investigation_type,
                    merged.extrQaqcStatus,
                    merged.ncbiProjectID,
                    merged.genomicsSampleID,
                    merged.sequenceAnalysisType,
                    merged.sampleMass,
                    merged.nucleicAcidConcentration,
                    merged.siteID,
                    merged.seqCollectDate,
                    merged.extrCollectDate,
                    afg.geneticSampleID,
                    afg.geneticFilteredSampleVolume,
                    afg.sampleMaterial,
                    afs.parentSampleID,
                    afs.namedLocation,
                    afs.decimalLatitude,
                    afs.decimalLongitude,
                    afs.elevation,
                    afs.aquaticSiteType,
                    afs.samplerType,
                    afs.dissolvedOxygen,
                    afs.specificConductance,
                    afs.waterTemp,
                    afs.lakeSampleDepth1,
                    afs.lakeSampleDepth2
                FROM 
                    (
                        SELECT
                            msq.collectDate AS seqCollectDate,
                            mde.collectDate AS extrCollectDate,
                            msq.laboratoryName,
                            msq.sequencingFacilityID,
                            msq.processedDate AS seqProcessedDate,
                            mde.processedDate AS extrProcessedDate,
                            msq.dnaSampleID,
                            msq.internalLabID,
                            msq.instrument_model,
                            msq.sequencingMethod,
                            msq.investigation_type,
                            mde.qaqcStatus AS extrQaqcStatus,
                            msq.ncbiProjectID,
                            msq.siteID,
                            msq.labPrepMethod,
                            mde.genomicsSampleID,
                            mde.sequenceAnalysisType,
                            mde.sampleMass,
                            mde.nucleicAcidConcentration,
                            mde.nucleicAcidQuantMethod,
                            mde.nucleicAcidPurity
                        FROM 
                            mms_swMetagenomeSequencing AS msq
                        JOIN 
                            mms_swMetagenomeDnaExtraction AS mde
                        ON 
                            msq.dnaSampleID = mde.dnaSampleID
                    ) AS merged
                JOIN amc_fieldGenetic AS afg
                ON
                    merged.genomicsSampleID = afg.geneticSampleID
                JOIN amc_fieldSuperParent AS afs
                ON
                    afg.parentSampleID = afs.parentSampleID
        """
        surface_water_samples = pd.read_sql_query(query, self.conn)

        # --------------------------------------------------
        # Create mappings for minted NMDC IDs
        # --------------------------------------------------
        neon_biosample_ids = surface_water_samples["parentSampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(neon_biosample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(neon_biosample_ids, nmdc_biosample_ids))

        neon_extraction_ids = surface_water_samples["parentSampleID"]
        nmdc_extraction_ids = self._id_minter(
            "nmdc:Extraction", len(neon_extraction_ids)
        )
        neon_to_nmdc_extraction_ids = dict(
            zip(neon_extraction_ids, nmdc_extraction_ids)
        )

        neon_extraction_processed_ids = surface_water_samples["parentSampleID"]
        nmdc_extraction_processed_ids = self._id_minter(
            "nmdc:ProcessedSample", len(neon_extraction_processed_ids)
        )
        neon_to_nmdc_extraction_processed_ids = dict(
            zip(neon_extraction_processed_ids, nmdc_extraction_processed_ids)
        )

        neon_lib_prep_ids = surface_water_samples["parentSampleID"]
        nmdc_lib_prep_ids = self._id_minter(
            "nmdc:LibraryPreparation", len(neon_lib_prep_ids)
        )
        neon_to_nmdc_lib_prep_ids = dict(zip(neon_lib_prep_ids, nmdc_lib_prep_ids))

        neon_lib_prep_processed_ids = surface_water_samples["parentSampleID"]
        nmdc_lib_prep_processed_ids = self._id_minter(
            "nmdc:ProcessedSample", len(neon_lib_prep_processed_ids)
        )
        neon_to_nmdc_lib_prep_processed_ids = dict(
            zip(neon_lib_prep_processed_ids, nmdc_lib_prep_processed_ids)
        )

        # --------------------------------------------------
        # STEP 1: Insert Biosamples
        # --------------------------------------------------
        for neon_id, nmdc_id in neon_to_nmdc_biosample_ids.items():
            biosample_row = surface_water_samples[
                surface_water_samples["parentSampleID"] == neon_id
            ]
            # database.biosample_set.append(
            #     self._translate_biosample(neon_id, nmdc_id, biosample_row)
            # )

        # --------------------------------------------------
        # STEP 2: Insert Extraction Processes
        # --------------------------------------------------
        for neon_id, nmdc_id in neon_to_nmdc_extraction_ids.items():
            extraction_row = surface_water_samples[
                surface_water_samples["parentSampleID"] == neon_id
            ]

            extraction_input = neon_to_nmdc_biosample_ids.get(neon_id)
            processed_sample_id = neon_to_nmdc_extraction_processed_ids.get(neon_id)

            if extraction_input is not None and processed_sample_id is not None:
                database.material_processing_set.append(
                    self._translate_extraction_process(
                        nmdc_id,
                        extraction_input,
                        processed_sample_id,
                        extraction_row,
                    )
                )

                genomics_sample_id = _get_value_or_none(
                    extraction_row, "genomicsSampleID"
                )

                # Each Extraction process output => ProcessedSample
                database.processed_sample_set.append(
                    self._translate_processed_sample(
                        processed_sample_id,
                        f"Extracted DNA from {genomics_sample_id}",
                    )
                )

        # --------------------------------------------------
        # STEP 3: Insert LibraryPreparation Processes
        # --------------------------------------------------
        for neon_id, nmdc_id in neon_to_nmdc_lib_prep_ids.items():
            lib_prep_row = surface_water_samples[
                surface_water_samples["parentSampleID"] == neon_id
            ]

            lib_prep_input = neon_to_nmdc_extraction_processed_ids.get(neon_id)
            processed_sample_id = neon_to_nmdc_lib_prep_processed_ids.get(neon_id)

            if lib_prep_input is not None and processed_sample_id is not None:
                database.material_processing_set.append(
                    self._translate_library_preparation(
                        nmdc_id,
                        lib_prep_input,
                        processed_sample_id,
                        lib_prep_row,
                    )
                )

                dna_sample_id = _get_value_or_none(lib_prep_row, "dnaSampleID")

                # Each LibraryPreparation process output => ProcessedSample
                database.processed_sample_set.append(
                    self._translate_processed_sample(
                        processed_sample_id,
                        f"Library preparation for {dna_sample_id}",
                    )
                )

        # --------------------------------------------------
        # STEP 4: Group raw files by (dnaSampleID, sequencerRunID)
        #         and insert DataObjects + DataGeneration processes
        # --------------------------------------------------
        raw_query = """
            SELECT dnaSampleID, sequencerRunID, rawDataFilePath
            FROM mms_swRawDataFiles
        """
        neon_raw_data_files_df = pd.read_sql_query(raw_query, self.conn)

        for neon_id, nmdc_libprep_id in neon_to_nmdc_lib_prep_ids.items():
            # 1) Pull out the row that corresponds to this parentSampleID
            lib_prep_row = surface_water_samples[
                surface_water_samples["parentSampleID"] == neon_id
            ]

            # 2) Grab the dnaSampleID from that row
            dna_sample_id = _get_value_or_none(lib_prep_row, "dnaSampleID")
            if not dna_sample_id:
                # No dnaSampleID => skip
                continue

            # 3) Find all raw files for that dnaSampleID
            dna_files = neon_raw_data_files_df[
                neon_raw_data_files_df["dnaSampleID"] == dna_sample_id
            ]
            if dna_files.empty:
                # No raw files => skip
                continue

            # -----------------------------------------
            # LOOKUP DICT: get "has_input" for this neon_id
            # -----------------------------------------
            has_input_value = self.samp_procsm_dict.get(neon_id)
            # If some neon_id isn't in the dictionary, handle it as needed
            if not has_input_value:
                # Could skip, or raise an error, or set a default
                continue

            # -------------------------------------------
            # 4) CREATE A MANIFEST IF MULTIPLE RAW FILES
            #    for this row's dnaSampleID
            # -------------------------------------------
            manifest_id = None
            if len(dna_files) > 2:
                # For each row that references a dnaSampleID with multiple raw files,
                # mint exactly one new manifest record
                manifest_id = self._id_minter("nmdc:Manifest", 1)[0]
                new_manifest = self._translate_manifest(manifest_id)
                # Add to the database
                database.manifest_set.append(new_manifest)

            # -------------------------------------------
            # 5) NOW GROUP FILES BY sequencerRunID
            #    => one data_generation record per run
            # -------------------------------------------
            lib_prep_processed_sample_id = neon_to_nmdc_lib_prep_processed_ids.get(
                neon_id
            )
            if not lib_prep_processed_sample_id:
                # If we don't have a ProcessedSample for some reason, skip
                continue

            for run_id, group_df in dna_files.groupby("sequencerRunID"):
                # a) Mint new data_generation (NucleotideSequencing) ID for this run
                data_generation_id = self._id_minter("nmdc:NucleotideSequencing", 1)[0]

                # b) Create DataObjects for each raw file in this run
                data_object_ids = []
                for raw_fp in group_df["rawDataFilePath"]:
                    do_id = self._id_minter("nmdc:DataObject", 1)[0]

                    # Distinguish read type
                    do_type = None
                    if "_R1.fastq.gz" in raw_fp:
                        do_type = "Metagenome Raw Read 1"
                    elif "_R2.fastq.gz" in raw_fp:
                        do_type = "Metagenome Raw Read 2"

                    # Create the DataObject
                    data_obj = self._translate_data_object(
                        do_id=do_id,
                        url=raw_fp,
                        do_type=do_type,
                        manifest_id=manifest_id,  # link to the new Manifest if it exists
                    )
                    database.data_object_set.append(data_obj)
                    data_object_ids.append(do_id)

                # c) Finally, create the data generation record for this run
                database.data_generation_set.append(
                    self._translate_nucleotide_sequencing(
                        nucleotide_sequencing_id=data_generation_id,
                        processed_sample_id=has_input_value,
                        raw_data_file_data=data_object_ids,
                        nucleotide_sequencing_row=lib_prep_row,
                    )
                )

        return database
