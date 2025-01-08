from urllib.parse import quote_plus, quote
import requests
import pandas as pd
import logging
import os
from datetime import datetime
import configparser
from authtoken import get_bearer_token

# Configure logging to log to a file with timestamps and log levels
logging.basicConfig(
    filename='application.log',
    filemode='w',  # Change to 'w' to overwrite each time
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_excel_engine(file_path):
    """
    Determines the appropriate Pandas Excel engine based on the file extension.

    Args:
        file_path (str): The path to the Excel file.

    Returns:
        str: The name of the engine to use with Pandas read_excel.

    Raises:
        ValueError: If the file extension is unsupported.
    """
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()

    if file_extension in ['.xlsx', '.xlsm']:
        return 'openpyxl'
    elif file_extension == '.xls':
        return 'xlrd'
    elif file_extension == '.xlsb':
        return 'pyxlsb'
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")

def load_config():
    config = configparser.ConfigParser()
    config.read('config.properties')
    environment = config.get('DEFAULT', 'ENVIRONMENT').lower()

    if environment == 'prod':
        raise ValueError(f"Unsupported environment '{environment}'. This script only supports DEV & UAT.")

    required_keys = ['api_url', 'excel_file_path']
    for key in required_keys:
        if not config.has_option('DEFAULT', key):
            raise ValueError(f"'{key}' is missing in the configuration.")

    return {
        'api_url': config.get('DEFAULT', 'api_url'),
        'excel_file_path': config.get('DEFAULT', 'excel_file_path')
    }

def save_to_excel(data, sheet_name, file_path):
    """Save the given data to a specified Excel sheet, using openpyxl to append if the file already exists."""
    df = pd.DataFrame(data)
    if not os.path.exists(file_path):
        # If the file does not exist, create a new one
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        # If the file exists, open it and add/update the sheet
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    logger.info(f"Saved data to '{sheet_name}' sheet in '{file_path}'")

def extract_disbursement_details(response_json, cms_agent, dealer_code, sale_date):
    """
    Extracts and validates disbursement details from the response JSON for each product type, product code,
    coverage code, and payee code level, including payee name and amount.
    Returns a tuple (results, all_conditions_met), where results contain extracted details with bucket validation status,
    and all_conditions_met is True if all validation conditions were met.
    """
    results = []

    # Agent-specific bucket configurations based on the provided table
    agent_bucket_info = {
        '001601': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '001602': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '010075': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '010000': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '010076': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '002031': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '009010': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '002300': {'override_bucket_from': 7, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '008100': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '002037': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '001950': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '001900': {'override_bucket_from': 15, 'override_bucket_to': 19, 'ncb_bucket_number': 9},
        '009000': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '002600': {'override_bucket_from': 4, 'override_bucket_to': 9, 'ncb_bucket_number': 8},
        '002700': {'override_bucket_from': 4, 'override_bucket_to': 9, 'ncb_bucket_number': 8},
        '002800': {'override_bucket_from': 4, 'override_bucket_to': 9, 'ncb_bucket_number': 8},
        '003000': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '003100': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '003200': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9},
        '000927' : {'override_bucket_from': 15, 'override_bucket_to': 20, 'ncb_bucket_number': 9},
        '003300': {'override_bucket_from': 4, 'override_bucket_to': 8, 'ncb_bucket_number': 9}
    }

    # Check if cms_agent is in the configuration
    if cms_agent not in agent_bucket_info:
        logger.error(f"Agent {cms_agent} is not in the agent bucket configuration.")
        return [], False

    # Retrieve agent-specific configurations
    agent_info = agent_bucket_info[cms_agent]
    override_bucket_from = agent_info['override_bucket_from']
    override_bucket_to = agent_info['override_bucket_to']
    ncb_bucket_number = agent_info['ncb_bucket_number']

    payee_buckets = {}
    all_conditions_met = True

    if 'overridesPayee' in response_json and 'productTypes' in response_json['overridesPayee']:
        logger.info(
            f"Processing disbursement data for dealerCode: {dealer_code}, agentCode: {cms_agent}, saleDate: {sale_date}"
        )

        for product in response_json['overridesPayee']['productTypes']:
            product_type = product['productType']
            product_type_description = product['productTypeDescription']
            product_code = product['productCode']
            logger.info(f"Processing productType: {product_type}, productCode: {product_code}")

            for commission in product.get('commission', []):
                payee_code = commission['payeeCode']
                coverage_code = commission['productCoverageCode']
                amount = commission['amount']
                actual_bucket_no = commission['agentBucket']

                bucket_validation_passed = True  # Initialize validation status for this commission
                ncbpayee = "CB"

                # Determine expected bucket_no based on payee_code and agent configuration
                if "NC" in payee_code:
                    ncbpayee = "NCB"
                    expected_bucket_no = ncb_bucket_number
                    if actual_bucket_no != expected_bucket_no:
                        logger.error(
                            f"Validation failed for NCB payeeCode '{payee_code}': "
                            f"Expected agentBucket={expected_bucket_no}, but got {actual_bucket_no}"
                        )
                        bucket_validation_passed = False
                        all_conditions_met = False
                    else:
                        logger.info(
                            f"Validation passed for NCB payeeCode '{payee_code}': agentBucket={actual_bucket_no}"
                        )
                else:
                    # Check if payee_code has been assigned a bucket already
                    if payee_code in payee_buckets:
                        expected_bucket_no = payee_buckets[payee_code]
                    else:
                        # Skip the ncb_bucket_number if it falls within the range
                        while override_bucket_from <= override_bucket_to:
                            if override_bucket_from == ncb_bucket_number:
                                logger.info(f"Skipping ncb_bucket_number {ncb_bucket_number} for agent {cms_agent}")
                                override_bucket_from += 1
                                continue
                            else:
                                break

                        # Assign the expected bucket number
                        expected_bucket_no = override_bucket_from
                        payee_buckets[payee_code] = override_bucket_from

                        # Increment override_bucket_from for next payee_code
                        override_bucket_from += 1

                        # Ensure that the bucket number does not exceed override_bucket_to
                        if override_bucket_from > override_bucket_to:
                            logger.warning(f"Bucket number exceeded override_bucket_to for agent {cms_agent}")
                            # Depending on your requirements, you may need to handle this scenario

                    # Validation check
                    if actual_bucket_no != expected_bucket_no:
                        logger.error(
                            f"Validation failed for payeeCode '{payee_code}': "
                            f"Expected agentBucket={expected_bucket_no}, but got {actual_bucket_no}"
                        )
                        bucket_validation_passed = False
                        all_conditions_met = False
                    else:
                        logger.info(f"Validation passed for payeeCode '{payee_code}': agentBucket={actual_bucket_no}")

                # Collect data into a result dictionary
                result = {
                    'cms_agent': cms_agent,
                    'dealer_code': dealer_code,
                    'sale_date': sale_date,
                    'product_type': product_type,
                    'product_type_description': product_type_description,
                    'product_code': product_code,
                    'coverage_code': coverage_code,
                    'payee_code': payee_code,
                    'bucket_no': actual_bucket_no,
                    'amount': amount,
                    'chargeback_status': ncbpayee,
                    'bucket_validation_passed': bucket_validation_passed
                }

                # Store amounts separately for NCB and CB payees
                if ncbpayee == 'NCB':
                    result['ncb_payee_amount'] = amount
                else:
                    result['override_payee_amount'] = amount

                results.append(result)

                # Log the extracted data
                logger.info(
                    f"Extracted Disbursement - Agent: {cms_agent}, Dealer: {dealer_code}, "
                    f"Product: {product_code}, Coverage: {coverage_code}, Payee: {payee_code}, "
                    f"Bucket: {actual_bucket_no}, Amount: {amount}, "
                    f"Bucket Validation Passed: {bucket_validation_passed}"
                )
    else:
        logger.warning(f"No disbursement details found for agentCode={cms_agent}, dealerCode={dealer_code}")
        return results, False

    # Final log based on validation outcome
    if all_conditions_met:
        logger.info(f"All validation conditions met for dealerCode: {dealer_code}, agentCode: {cms_agent}")
    else:
        logger.error(f"Validation failed for one or more entries in dealerCode: {dealer_code}, agentCode: {cms_agent}")

    return results, all_conditions_met

def perform_disbursement_api_requests(config):
    token = get_bearer_token()
    if not token:
        logger.error("Failed to retrieve bearer token.")
        return []

    file_path = config['excel_file_path']
    try:
        engine = get_excel_engine(file_path)  # Determine the appropriate engine
    except ValueError as ve:
        logger.error(ve)
        return []

    try:
        df = pd.read_excel(file_path, dtype={'cms_agent': str}, engine=engine)
    except Exception as e:
        logger.error(f"Failed to read Excel file '{file_path}'. Error: {e}")
        return []

    logger.info(f"Columns in the Excel file: {df.columns.tolist()}")
    disbursement_url = config['api_url'] + "/sidecar/dealer/overridesPayee"
    all_results = []

    for index, row in df.iterrows():
        cms_agent = row.get('cms_agent')
        dealer_code = row.get('dealer_code')
        product_type = row.get('product_type')
        current_date = row['current_date'].strftime('%Y-%m-%d')
        product_code = row.get('product_code')
        coverage_code = row.get('osc_coverage_code')

        # Mandatory parameters
        params = {
            'saleDate': current_date,
            'dealercode': dealer_code
        }

        # Optional parameters: include only if they have values
        if pd.notna(cms_agent) and cms_agent:
            cms_agent = cms_agent.zfill(6)
            params['agentCode'] = cms_agent
        if pd.notna(product_code) and product_code:
            params['productCode'] = product_code  # Keep the comma unencoded
        if pd.notna(coverage_code) and coverage_code:
            params['coverageCode'] = coverage_code
        if pd.notna(product_type) and product_type:
            params['productType'] = product_type

        # Manually build the query string to prevent encoding commas in productCode
        query_params = []
        for key, value in params.items():
            if key == 'productCode':
                # Encode value but do not encode commas
                encoded_value = quote(str(value), safe=',')
            else:
                # Encode other parameters normally
                encoded_value = quote(str(value), safe='')
            query_params.append(f"{key}={encoded_value}")

        query_string = '&'.join(query_params)
        full_url = f"{disbursement_url}?{query_string}"

        headers = {'Authorization': f'Bearer {token}'}
        logger.info(f"Requesting URL: {full_url}")

        response = None
        try:
            response = requests.get(full_url, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            logger.info(f"Response received: {response_json}")

            disbursement_data, validation_status = extract_disbursement_details(
                response_json, cms_agent, dealer_code, current_date
            )
            all_results.extend(disbursement_data)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for dealerCode={dealer_code}. Error: {e}")
            if response is not None and response.text:
                logger.error(f"Response content: {response.text}")

    save_to_excel(all_results, "Disbursement Data", file_path)
    return all_results

def perform_api_requests(config):
    token = get_bearer_token()
    if not token:
        logger.error("Failed to retrieve bearer token.")
        return []

    file_path = config['excel_file_path']
    try:
        engine = get_excel_engine(file_path)  # Determine the appropriate engine
    except ValueError as ve:
        logger.error(ve)
        return []

    try:
        df = pd.read_excel(file_path, dtype={'cms_agent': str}, engine=engine)
    except Exception as e:
        logger.error(f"Failed to read Excel file '{file_path}'. Error: {e}")
        return []

    logger.info(f"Columns in the Excel file: {df.columns.tolist()}")
    overrides_url = config['api_url'] + "/sidecar/dealer/overrides"
    all_results = []

    for index, row in df.iterrows():
        cms_agent = row.get('cms_agent')
        dealer_code = row.get('dealer_code')
        product_type = row.get('product_type')
        current_date = row['current_date'].strftime('%Y-%m-%d')
        product_code = row.get('product_code')
        coverage_code = row.get('osc_coverage_code')

        # Mandatory parameters
        params = {
            'saleDate': current_date,
            'dealercode': dealer_code
        }

        # Optional parameters: include only if they have values
        if pd.notna(cms_agent) and cms_agent:
            cms_agent = cms_agent.zfill(6)
            params['agentCode'] = cms_agent
        if pd.notna(product_code) and product_code:
            params['productCode'] = product_code  # Keep the comma unencoded
        if pd.notna(coverage_code) and coverage_code:
            params['coverageCode'] = coverage_code
        if pd.notna(product_type) and product_type:
            params['productType'] = product_type

        # Manually build the query string to prevent encoding commas in productCode
        query_params = []
        for key, value in params.items():
            if key == 'productCode':
                # Encode value but do not encode commas
                encoded_value = quote(str(value), safe=',')
            else:
                # Encode other parameters normally
                encoded_value = quote(str(value), safe='')
            query_params.append(f"{key}={encoded_value}")

        query_string = '&'.join(query_params)
        full_url = f"{overrides_url}?{query_string}"

        headers = {'Authorization': f'Bearer {token}'}
        logger.info(f"Requesting URL: {full_url}")

        response = None
        try:
            response = requests.get(full_url, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            logger.info(f"Response received: {response_json}")

            extracted_data = extract_override_amounts(response_json, cms_agent, dealer_code, current_date)
            all_results.extend(extracted_data)

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed overrides request for dealerCode={dealer_code}. Error: {e}"
            )
            if response is not None and response.text:
                logger.error(f"Response content: {response.text}")

    save_to_excel(all_results, "Override Data", file_path)
    return all_results

def extract_override_amounts(response_json, cms_agent, dealer_code, sale_date):
    results = []

    if 'dealerOverrides' in response_json and 'productTypes' in response_json['dealerOverrides']:
        for product in response_json['dealerOverrides']['productTypes']:
            product_type = product['productType']
            product_code = product['productCode']
            coverage_code = product['productCoverageCode']
            amount = product['amount']

            result = {
                'cms_agent': cms_agent,
                'dealer_code': dealer_code,
                'sale_date': sale_date,
                'product_type': product_type,
                'product_code': product_code,
                'coverage_code': coverage_code,
                'amount': amount
            }
            results.append(result)

            logger.info(
                f"Extracted Override - Agent: {cms_agent}, Dealer: {dealer_code}, "
                f"Product: {product_code}, Coverage: {coverage_code}, Amount: {amount}"
            )
    else:
        logger.warning(f"No product types found for agentCode={cms_agent}, dealerCode={dealer_code}")

    return results

def perform_chargeback_percentage_request(config):
    token = get_bearer_token()
    if not token:
        logger.error("Failed to retrieve bearer token.")
        return []

    file_path = config['excel_file_path']
    try:
        engine = get_excel_engine(file_path)  # Determine the appropriate engine
    except ValueError as ve:
        logger.error(ve)
        return []

    try:
        df = pd.read_excel(file_path, dtype={'cms_agent': str}, engine=engine)
    except Exception as e:
        logger.error(f"Failed to read Excel file '{file_path}'. Error: {e}")
        return []

    logger.info(f"Columns in the Excel file: {df.columns.tolist()}")
    chargeback_url = config['api_url'] + "/trpc/chargebackPercentage.get"
    all_results = []

    for index, row in df.iterrows():
        cms_agent = row['cms_agent'].zfill(6)

        params = {'input': f'"{cms_agent}"'}  # Ensure the agent code is in quotes as in the sample

        headers = {'Authorization': f'Bearer {token}'}
        logger.info(f"Requesting URL: {chargeback_url} with params {params}")

        response = None
        try:
            response = requests.get(chargeback_url, params=params, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            logger.info(f"Response received: {response_json}")

            chargeback_data = response_json.get("result", {}).get("data", {}).get("chargebackData", [])
            for item in chargeback_data:
                product_type_description = item.get("productTypeDescription")
                product_type = item.get("productType")
                percentage = item.get("percentage")

                result = {
                    'cms_agent': cms_agent,
                    'product_type': product_type,
                    'product_type_description': product_type_description,
                    'percentage': percentage
                }
                all_results.append(result)
                logger.info(
                    f"Extracted Chargeback Percentage - Agent: {cms_agent}, "
                    f"Product Type: {product_type}, Percentage: {percentage}"
                )

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for agentCode={cms_agent}. Error: {e}")
            if response is not None and response.text:
                logger.error(f"Response content: {response.text}")

    save_to_excel(all_results, "Chargeback Percentage", file_path)
    return all_results

# NEW FUNCTION TO PROCESS COMMISSIONS AND COMPARE AMOUNTS
def process_commissions(config):
    """
    Processes commission data by calculating NCB fees and override amounts,
    then compares them with Disbursement API responses.
    """
    file_path = config['excel_file_path']
    engine = 'openpyxl'  # Specify the engine directly

    try:
        # Read the first sheet to get dealer_code and chargeback_status
        xls = pd.ExcelFile(file_path, engine=engine)  # Specify engine here
        first_sheet_name = xls.sheet_names[0]
        dealer_status_df = pd.read_excel(
            xls,
            sheet_name=first_sheet_name,
            dtype={'dealer_code': str},
            engine=engine
        )

        # Create a mapping from dealer_code to chargeback_status
        dealer_status_mapping = dealer_status_df.set_index('dealer_code')['chargeback_status'].to_dict()

        # Read commission data from the Override Data sheet
        commission_df = pd.read_excel(
            file_path,
            sheet_name='Override Data',
            dtype={'cms_agent': str, 'dealer_code': str, 'product_code': str, 'coverage_code': str},
            engine=engine
        )
        commission_df['sale_date'] = pd.to_datetime(commission_df['sale_date'])

        # Load chargeback percentages from the Chargeback Percentage sheet
        chargeback_df = pd.read_excel(
            file_path,
            sheet_name='Chargeback Percentage',
            dtype={'cms_agent': str, 'product_type': str, 'product_type_description': str},
            engine=engine
        )

        # Standardize product_type_description in chargeback_df
        chargeback_df['product_type_description'] = chargeback_df['product_type_description'].apply(
            standardize_product_type_description
        )

        # Remove duplicates from chargeback_df
        chargeback_df = chargeback_df.drop_duplicates(subset=['cms_agent', 'product_type'])

        # Create a lookup dictionary for chargeback percentages based on (cms_agent, product_type)
        chargeback_percentages = chargeback_df.set_index(['cms_agent', 'product_type'])['percentage'].to_dict()

        # Load disbursement data from the Disbursement Data sheet
        disbursement_df = pd.read_excel(
            file_path,
            sheet_name='Disbursement Data',
            dtype={'cms_agent': str, 'dealer_code': str, 'product_code': str,
                   'coverage_code': str, 'payee_code': str},
            engine=engine
        )
        disbursement_df['sale_date'] = pd.to_datetime(disbursement_df['sale_date'])

        comparison_results = []

        for index, row in commission_df.iterrows():
            cms_agent = row['cms_agent'].zfill(6)
            dealer_code = row['dealer_code']
            sale_date = row['sale_date']
            product_type = row['product_type']
            product_code = row['product_code']
            coverage_code = row['coverage_code']
            commission_amount = row['amount']

            # Get dealer_status from dealer_status_mapping
            dealer_status = dealer_status_mapping.get(dealer_code, 'CB')  # Default to 'CB' if not found

            # Fetch the disbursement records matching the current commission record
            disbursement_records = disbursement_df[
                (disbursement_df['cms_agent'] == cms_agent) &
                (disbursement_df['dealer_code'] == dealer_code) &
                (disbursement_df['sale_date'] == sale_date) &
                (disbursement_df['product_type'] == product_type) &
                (disbursement_df['product_code'] == product_code)
            ].copy()  # Explicitly create a copy

            # If coverage_code is not empty, include it in the matching criteria
            if pd.notna(coverage_code) and coverage_code:
                disbursement_records = disbursement_records[
                    disbursement_records['coverage_code'] == coverage_code
                ].copy()  # Explicitly create a copy

            if disbursement_records.empty:
                logger.warning(
                    f"No disbursement data found for agent {cms_agent}, dealer {dealer_code}, product {product_code}"
                )
                continue  # Skip to next record

            # Assuming all records have the same product_type
            product_type = disbursement_records.iloc[0]['product_type']

            # Fetch the chargeback percentage using (cms_agent, product_type)
            percentage_key = (cms_agent, product_type)
            chargeback_percentage = chargeback_percentages.get(percentage_key, 0.0)
            if chargeback_percentage == 0.0:
                logger.warning(
                    f"No chargeback percentage found for agent {cms_agent}, product type '{product_type}'"
                )

            # Sum amounts from disbursement data
            total_disbursement_amount = disbursement_records['amount'].sum()

            # Ensure payee_code is a string and handle missing values
            disbursement_records.loc[:, 'payee_code'] = disbursement_records['payee_code'].astype(str).fillna('')

            # Identify NCB payee amounts dynamically
            disbursement_records.loc[:, 'is_ncb_payee'] = disbursement_records['payee_code'].str.contains('NC', case=False, na=False)

            # Sum the NCB payee amount
            ncb_payee_amount = disbursement_records.loc[disbursement_records['is_ncb_payee'], 'amount'].sum()

            # The override payee amount is the total amount minus the NCB payee amount
            override_payee_amount = total_disbursement_amount - ncb_payee_amount

            # Verify NCB payee existence based on dealer_status
            if dealer_status == 'NCB':
                if ncb_payee_amount == 0.0:
                    logger.error(
                        f"Dealer {dealer_code} is NCB but there is no NCB payee amount in disbursement records."
                    )
                    continue  # Skip to next record
                # Calculate NCB Fee and override amount
                ncb_fee = (commission_amount * chargeback_percentage) / 100
                override_amount = commission_amount - ncb_fee
            else:  # dealer_status == 'CB'
                if ncb_payee_amount > 0.0:
                    logger.error(
                        f"Dealer {dealer_code} is CB but NCB payee amount exists in disbursement records."
                    )
                    continue  # Skip to next record
                # For CB dealers, the override amount is the commission amount
                override_amount = commission_amount
                ncb_fee = 0.0

            # Compare calculated amounts with disbursement amounts
            amounts_match = compare_amounts(
                dealer_status, ncb_fee, override_amount, override_payee_amount, ncb_payee_amount
            )

            # Include chargeback_percentage in the result if desired
            result = {
                'cms_agent': cms_agent,
                'dealer_code': dealer_code,
                'sale_date': sale_date,
                'product_type': product_type,
                'product_code': product_code,
                'coverage_code': coverage_code,
                'dealer_status': dealer_status,
                'commission_amount': commission_amount,
                'ncb_fee': ncb_fee,
                'override_amount': override_amount,
                'override_payee_amount': override_payee_amount,
                'ncb_payee_amount': ncb_payee_amount,
                'chargeback_percentage': chargeback_percentage,  # Added chargeback_percentage
                'amounts_match': amounts_match
            }
            comparison_results.append(result)

    except Exception as e:
        logger.error(f"An error occurred while processing commissions: {e}")
        return []

    # Convert comparison_results to DataFrame
    result_df = pd.DataFrame(comparison_results)

    # Drop duplicate rows based on all columns
    result_df = result_df.drop_duplicates()

    # Alternatively, drop duplicates based on a subset of columns
    # Uncomment and adjust the subset list as needed
    # subset_columns = ['cms_agent', 'dealer_code', 'sale_date', 'product_code']
    # result_df = result_df.drop_duplicates(subset=subset_columns, keep='first')

    # Save the comparison results to a new sheet
    save_to_excel(result_df, "Commission Comparison", file_path)
    logger.info("Commission comparison completed and saved to Excel.")
    return result_df


def standardize_product_type_description(description):
    """
    Standardizes the product_type_description to match keys in chargeback_percentages.
    """
    # Convert to uppercase and remove extra spaces
    return ' '.join(str(description).upper().split())

def compare_amounts(dealer_status, ncb_fee, override_amount, override_payee_amount, ncb_payee_amount):
    """
    Compares calculated amounts with disbursement amounts from the API response.
    Returns True if amounts match, False otherwise.
    """
    tolerance = 0.01  # Allow small tolerance for floating-point comparison

    if dealer_status == 'NCB':
        # For NCB, compare both NCB Fee and override amount
        ncb_match = abs(ncb_fee - ncb_payee_amount) < tolerance
        override_match = abs(override_amount - override_payee_amount) < tolerance
        if not ncb_match:
            logger.error(f"NCB Fee mismatch: Calculated {ncb_fee}, Disbursement {ncb_payee_amount}")
        if not override_match:
            logger.error(
                f"Override amount mismatch: Calculated {override_amount}, Disbursement {override_payee_amount}"
            )
        return ncb_match and override_match
    else:
        # For CB, compare the override amount
        override_match = abs(override_amount - override_payee_amount) < tolerance
        if not override_match:
            logger.error(
                f"Override amount mismatch: Calculated {override_amount}, Disbursement {override_payee_amount}"
            )
        return override_match

def main():
    try:
        config = load_config()
        results = perform_api_requests(config)
        logger.info(f"Results from Override API requests: {results}")
        disbursement_results = perform_disbursement_api_requests(config)
        logger.info(f"Results from Disbursement API requests: {disbursement_results}")
        config_percentage = perform_chargeback_percentage_request(config)
        logger.info(f"Results from Chargeback Percentage API requests: {config_percentage}")

        # Process commissions and compare amounts
        comparison_results = process_commissions(config)
        logger.info(f"Commission comparison results: {comparison_results}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
