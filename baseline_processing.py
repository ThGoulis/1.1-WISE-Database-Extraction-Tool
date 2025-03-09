import os
import time
import baseline_extraction
import argparse
from multiprocessing import Pool, cpu_count
from tqdm import tqdm # type: ignore


def run_function(task):
    desc, func, args = task
    try:
        tqdm.write(f"Starting: {desc}...")
        func(*args)
        return desc, True, time.time()
    except Exception as e:
        return desc, False, str(e)


def run_csv_generation_process_multiprocessing(db_file, countryCode, working_directory):
    """ Runs all extraction functions in parallel using multiprocessing """
    
    os.makedirs(working_directory, exist_ok=True)

    baseline_extraction.create_and_populate_swRBD_Europe_data(db_file)
    
    baseline_extraction.updateTables(db_file)

    functions = [
        ("Generating RBD Code Names",baseline_extraction.rbdCodeNames, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Body Table",baseline_extraction.WISE_SOW_SurfaceWaterBody_SWB_Table, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Body Categories",baseline_extraction.WISE_SOW_SurfaceWaterBody_SWB_Category, (db_file, countryCode, 2016, working_directory)),
        ("Ecological Exemptions & Pressures",baseline_extraction.Surface_water_bodies_Ecological_exemptions_and_pressures, (db_file, countryCode, 2016, working_directory)),
        ("Quality Element Exemption Type",baseline_extraction.Surface_water_bodies_Quality_element_exemptions_Type, (db_file, countryCode, 2016, working_directory)),
        ("Chemical Exemption Type",baseline_extraction.SWB_Chemical_exemption_type, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Body Chemical Status",baseline_extraction.WISE_SOW_SurfaceWaterBody_SWB_ChemicalStatus_Table, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Body Chemical Status by Category",baseline_extraction.SurfaceWaterBody_ChemicalStatus_Table_by_Category, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Good-High Status",baseline_extraction.Surface_water_bodies_Ecological_status_or_potential_groupGoodHigh, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Failing Status",baseline_extraction.Surface_water_bodies_Ecological_status_or_potential_groupFailling, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Rivers and Lakes Status",baseline_extraction.swEcologicalStatusOrPotential_RW_LW_Category2ndRBMP2016, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Chemical Status by Country by category",baseline_extraction.swEcologicalStatusOrPotentialValue_swChemicalStatusValue_by_Country_by_Categ, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Chemical assesment using monitoring",baseline_extraction.swb_Chemical_assessment_using_monitoring_grouping_or_expert_judgement, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Pollutants",baseline_extraction.swRBsPollutants, (db_file, countryCode, 2016, working_directory)),
        ("Ecological Expected Good in 2015",baseline_extraction.swEcologicalStatusOrPotentialExpectedGoodIn2015, (db_file, countryCode, 2016, working_directory)),
        ("Ecological Expected Achievement Date",baseline_extraction.swEcologicalStatusOrPotentialExpectedAchievementDate, (db_file, countryCode, 2016, working_directory)),
        ("Chemical Expected Good in 2015",baseline_extraction.swChemicalStatusExpectedGoodIn2015, (db_file, countryCode, 2016, working_directory)),
        ("Chemical Expected Achievement Date",baseline_extraction.swChemicalStatusExpectedAchievementDate, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Body Category 2016",baseline_extraction.GroundWaterBodyCategory2016, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Exemptions",baseline_extraction.Groundwater_bodies_Chemical_Exemption_Type, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Quantitative Exemption",baseline_extraction.Groundwater_bodies_Quantitative_Exemption_Type, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Exemptions",baseline_extraction.gwChemical_exemptions_and_pressures, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Quantitative & Pressures",baseline_extraction.Groundwater_bodies_Quantitative_exemptions_and_pressures, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Status",baseline_extraction.SOW_GWB_GroundWaterBody_GWB_Chemical_status, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Quantitative Status",baseline_extraction.SOW_GWB_GroundWaterBody_GWB_Quantitative_status, (db_file, countryCode, 2016, working_directory)),
        ("Quantitative vs Chemical Status",baseline_extraction.gwQuantitativeStatusValue_gwChemicalStatusValue, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater At Risk",baseline_extraction.Groundwater_bodies_At_risk_of_failing_to_achieve_good_quantitative_status, (db_file, countryCode, 2016, working_directory)),
        ("Quantitative vs Chemical Status",baseline_extraction.SOW_GWB_gwQuantitativeReasonsForFailure_Table, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Reason for failure",baseline_extraction.SOW_GWB_gwChemicalReasonsForFailure_Table, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Status Values",baseline_extraction.gwChemicalStatusValue_Table, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Quantitatives Status",baseline_extraction.gwQuantitativeStatusExpectedGoodIn2015, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Quantitatives Status Expected Achievement Date ",baseline_extraction.gwQuantitativeStatusExpectedAchievementDate, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Status Expected Good in 2015",baseline_extraction.gwChemicalStatusExpectedGoodIn2015, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Status Expected Achievement Date",baseline_extraction.gwChemicalStatusExpectedAchievementDate, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Quantitatives Assessment Confidence",baseline_extraction.gwQuantitativeAssessmentConfidence, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Chemical Assessment Confidence",baseline_extraction.gwChemicalAssessmentConfidence, (db_file, countryCode, 2016, working_directory)),
        ("Number of groundwaters bodies failing to achive good status",baseline_extraction.Number_of_groundwater_bodies_failing_to_achieve_good_status, (db_file, countryCode, 2016, working_directory)),
        ("Geological Formations",baseline_extraction.geologicalFormation, (db_file, countryCode, 2016, working_directory)),
        ("Surface water Number of Impacts by Country",baseline_extraction.swNumber_of_Impacts_by_country, (db_file, countryCode, 2016, working_directory)),
        ("Surface water Presure Type",baseline_extraction.swSignificant_Pressure_Type_Table2016, (db_file, countryCode, 2016, working_directory)),
        ("Significant Impact Type",baseline_extraction.SignificantImpactType_Table2016, (db_file, countryCode, 2016, working_directory)),
        ("Surface water Significant Impacts type Other",baseline_extraction.swSignificantImpactType_Table_Other2016, (db_file, countryCode, 2016, working_directory)),
        ("Surface water Significant Pressure Other",baseline_extraction.swSignificantPressureType_Table_Other, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Significant Impact type",baseline_extraction.gwSignificantImpactTypeByCountry, (db_file, countryCode, 2016, working_directory)),
        ("Significant Impact Type 2016",baseline_extraction.gwSignificantImpactType2016, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Significant Impact type Other",baseline_extraction.gwSignificantImpactType_Other, (db_file, countryCode, 2016, working_directory)),
        ("Significant Pressure Type by Country",baseline_extraction.SOW_GWB_gwSignificantPressureType_NumberOfImpact_by_country, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Significant Pressure type",baseline_extraction.gwSignificantPressureType2016, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Significant Pressure type Other",baseline_extraction.gwSignificantPressureType_OtherTable2016, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Pollutants",baseline_extraction.SOW_GWB_gwPollutant_Table, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Significant Pressure type",baseline_extraction.SOW_GWB_gwPollutant_Table_Other, (db_file, countryCode, 2016, working_directory)),
        ("Surface water specific pollutant reported as Other",baseline_extraction.swRiver_basin_specific_pollutants_reported_as_Other, (db_file, countryCode, 2016, working_directory)),
        ("Groundwater Bodies Failing by Country",baseline_extraction.Ground_water_bodies_Failing_notUnknown_by_Country, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Bodies Failing by Country",baseline_extraction.Surface_water_bodies_Failing_notUnknown_by_Country, (db_file, countryCode, 2016, working_directory)),
        ("Surface water QE1",baseline_extraction.Surface_water_bodies_QE1_Biological_quality_elements_assessment, (db_file, countryCode, 2016, working_directory)),
        ("Surface water QE2",baseline_extraction.Surface_water_bodies_QE2_assessment, (db_file, countryCode, 2016, working_directory)),
        ("Surface water QE3",baseline_extraction.Surface_water_bodies_QE3_assessment, (db_file, countryCode, 2016, working_directory)),
        ("Surface water QE3.3",baseline_extraction.Surface_water_bodies_QE3_3_assessment, (db_file, countryCode, 2016, working_directory)),
        ("Surface water Delineation of the management",baseline_extraction.sw_delineation_of_the_management_units_in_the_1st_and_2nd_RBMP, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Chemical Status by Country",baseline_extraction.swChemical_by_Country_2016, (db_file, countryCode, 2016, working_directory)),
        ("Ecological Exemption Type",baseline_extraction.Surface_water_bodies_Ecological_exemptions_Type, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Unknown Category",baseline_extraction.swEcologicalStatusOrPotential_Unknown_Category2ndRBMP2016, (db_file, countryCode, 2016, working_directory)),
        ("Surface Water Chemical Status by Country",baseline_extraction.swEcologicalStatusOrPotentialChemical_by_Country, (db_file, countryCode, 2016, working_directory)),
        
    ]

    num_workers = max(1, cpu_count() - 1)
    print(f"🔄 Running {len(functions)} tasks with {num_workers} workers...")

    with Pool(processes=num_workers) as pool:
        results = list(tqdm(pool.imap(run_function, functions), total=len(functions), desc="Processing CSV", unit="task"))

    for desc, success, info in results:
        print(f"✅ {desc} completed." if success else f"⚠️ {desc} failed: {info}")


# Command-line argument parsing
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract data from WISE database and generate CSV reports')
    parser.add_argument('db', help='Path to SQLite DB file')
    parser.add_argument('country', help='Country Code for extraction')
    parser.add_argument('outputdir', help='Directory for CSV outputs (must NOT end with a backslash \\)')
    
    args = parser.parse_args()

    # Define parameters

    countryCode = [args.country]

    country = ' '.join(countryCode)

    working_directory = os.path.join(args.outputdir, country)

    if not os.path.exists(args.db):
        raise FileNotFoundError(f"Database file not found: {args.db}")
    
    input("Do you want to create indexes for database? (y/n): ").strip().lower()

    if input("Do you want to create indexes for database? (y/n): ").strip().lower() == "y":
        baseline_extraction.createIndexies(args.db)

    # Run extraction process in parallel
    start_time = time.time()
    run_csv_generation_process_multiprocessing(args.db, countryCode, working_directory)
    elapsed_time = time.time() - start_time

    print(f"⏳ Total Execution Time: {elapsed_time:.2f} seconds")
