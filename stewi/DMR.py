#!/usr/bin/env python
# Queries DMR data by SIC or by SIC and Region (for large sets), temporarily saves them,
# Web service documentation found at https://echo.epa.gov/system/files/ECHO%20All%20Data%20Search%20Services_v3.pdf


import os, requests
import pandas as pd
import stewi.globals as globals
from stewi.globals import set_dir

data_source = 'dmr'
output_dir = globals.output_dir
data_dir = globals.data_dir
dmr_external_dir = set_dir(data_dir + '/../../../DMR Data Files')


# appends form object to be used in query to sic code
def app_param(form_obj, param_list):
    result = [form_obj + s for s in param_list]
    return result


# creates urls from various search parameters and outputs as a list
def create_urls(main_api, service_parameter, year, l, output_type, region=[], responseset='100000',nutrient_agg=True):
    urls = []
    for s in l:
        if region:
            for r in region:
                url = main_api + service_parameter + year + s + r
                if responseset: url += '&responseset=' + responseset
                if nutrient_agg:
                    url += '&p_nutrient_agg=Y'
                url += '&output=' + output_type
                urls.append(url)
        else:
            url = main_api + service_parameter + year + s
            if responseset: url += '&responseset=' + responseset
            if nutrient_agg:
                url += '&p_nutrient_agg=Y'
            url += '&output=' + output_type
            urls.append(url)
    return urls


def query_dmr(urls, sic_list=[], reg_list=[], path=''):
    output_df = pd.DataFrame()
    max_error_list = []
    no_data_list = []
    success_list = []
    if len(urls) != len(sic_list): sic_list = [[s, r] for s in sic_list for r in reg_list]
    for i in range(len(urls)):
        if sic_list[i] in (['12', 'KY'], ['12', 'WV']): continue #No data for SIC2 93? -- double-check
        # final_path = path + 'sic_' + sic_list[i] + '.json'
        final_path = path + 'sic_' + str(sic_list[i]) + '.pickle'
        print(final_path)
        if len(sic_list) == len(urls): print(sic_list[i])
        if os.path.exists(final_path):
            print(final_path)
            df = pd.read_pickle(final_path)
            df = pd.DataFrame(df['Results']['Results'])
            output_df = pd.concat([output_df, df])
            if len(sic_list) == len(urls): success_list.append(sic_list[i])
            continue
        while True:
            try:
                print(urls[i])
                json_data = requests.get(urls[i]).json()
                df = pd.DataFrame(json_data)
                break
            except: pass
            #Exception handling for http 500 server error still needed
        if 'Error' in df.index:
            if df['Results'].astype(str).str.contains('Maximum').any():
                # print("iterate through by region code" + url)
                # split url by & and append region code, import function debugging
                if len(sic_list) == len(urls): max_error_list.append(sic_list[i])
            else: print("Error: " + urls[i])
        elif 'NoDataMsg' in df.index:
            if len(sic_list) == len(urls):
                print('No data found for'': ' + urls[i])
                no_data_list.append(sic_list[i])
        else:
            # with open(final_path, 'w') as fp:
            # json.dump(json_data, fp, indent = 2)
            pd.to_pickle(df, final_path)
            df = pd.DataFrame(df['Results']['Results'])
            output_df = pd.concat([output_df, df])
            if len(sic_list) == len(urls): success_list.append(sic_list[i])
    return output_df, max_error_list, no_data_list, success_list


def query_paginated(sic, url, state='', path=''):
    output_df = pd.DataFrame()
    max_error_list = []
    no_data_list = []
    success_list = []
    counter = 1
    pages = 1
    url += '&p_st=' + state
    while counter <= pages:
        page = sic + '_' + str(counter)
        print(page + '_' + state)
        final_path = path + page + '_' + state + '.pickle'
        if os.path.exists(final_path):
            df = pd.read_pickle(final_path)
            if counter == 1: pages = int(df['Results']['PageCount'])
            df = pd.DataFrame(df['Results']['Results'])
            output_df = pd.concat([output_df, df])
            counter += 1
            continue
        temp_url = url + '&pageno=' + str(counter)
        print(temp_url)
        while True:
            try:
                json_data = requests.get(temp_url).json()
                df = pd.DataFrame(json_data)
                if counter == 1: pages = int(df['Results']['PageCount'])
                break
            except: pass
        if 'Error' in df.index:
            if df['Results'].astype(str).str.contains('Maximum').any():
                max_error_list.append(page + '_' + state)
            else:
                print("Error: " + temp_url)
        elif 'NoDataMsg' in df.index:
                print('No data found for'': ' + temp_url)
                no_data_list.append(page + '_' + state)
        else:
            # with open(final_path, 'w') as fp:
            # json.dump(json_data, fp, indent = 2)
            pd.to_pickle(df, final_path)
            df = pd.DataFrame(df['Results']['Results'])
            output_df = pd.concat([output_df, df])
            success_list.append(page + '_' + state)
        counter += 1
    print(max_error_list, no_data_list, success_list)
    return output_df


# creates file path for json output.
# iterates through url list, requests data, and writes to json file in output directory.
def main():
    # two digit SIC codes from advanced search drop down stripped and formatted as a list
    report_year = '2016'  # year of data requested
    sic = ['01', '02', '07', '08', '09', '10', '12', '13', '14', '15',
           '16', '17', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29',
           '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41',
           '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53',
           '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65',
           '67', '70', '72', '73', '75', '76', '78', '79', '80', '81', '82', '83',
           '84', '86', '87', '88', '89', '91', '92', '93', '95', '96', '97', '99']
    epa_region = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
    states_df = pd.read_csv(data_dir + 'state_codes.csv')
    states = list(states_df['states']) + list(states_df['dc']) + list(states_df['territories'])
    states = [x for x in states if str(x) != 'nan']
    main_api = 'https://ofmpub.epa.gov/echo/dmr_rest_services.get_custom_data_'  # base url
    service_parameter = 'annual?'  # define which parameter is primary search criterion
    year = 'p_year=' + report_year  # define year
    form_obj_sic = '&p_sic2='# define any secondary search criteria
    form_obj_sic4 = '&p_sic4='
    form_obj_reg = '&p_reg='
    form_obj_st = '&p_st='
    output_type = 'JSON'  # define output type

    # dmr_df = pd.DataFrame()

    sic_code_query = app_param(form_obj_sic, sic)
    # output_dir = set_output_dir('./output/DMRquerybySIC/')
    urls = create_urls(main_api, service_parameter, year, sic_code_query,
                       output_type)  # creates a list oof urls based on sic
    # json_output_file = get_write_json_file(urls, output_dir, 'DMR_data') #saves json file to LCI-Prime_Output
    print(report_year)
    dmr_df, sic_maximum_record_error_list, sic_no_data_list, sic_successful_df_list = \
        query_dmr(urls, sic, path=dmr_external_dir)#output_dir
    if sic_successful_df_list: print('Successfully obtained data for the following SIC:\n' +
                                           str(sic_successful_df_list))
    if sic_no_data_list: print('No data for the following SIC:\n' + str(sic_no_data_list))
    if sic_maximum_record_error_list: print('Maximum record limit reached for the following SIC:\n' +
                                            str(sic_maximum_record_error_list) +
                                            '\nBreaking queries up by State...\n')

    max_error_sic_query = app_param(form_obj_sic, sic_maximum_record_error_list)
    states_query = app_param(form_obj_st, states)
    states_urls = create_urls(main_api, service_parameter, year, max_error_sic_query, output_type, states_query)
    st_df, st_max_error, st_no_data, st_success = \
        query_dmr(states_urls, sic_maximum_record_error_list, states, path=dmr_external_dir)  # output_dir
    if st_success: print('Successfully obtained data for [SIC, State]:\n' + str(st_success))
    if st_no_data: print(('No data for [SIC, State]:\n' + str(st_no_data)))
    if st_max_error:
        print('Maximum record limit still reached for the following [SIC, State]:\n' + str(st_max_error))
    print('Paginating remaining SIC-State combinations...')
    dmr_df = pd.concat([dmr_df, st_df])

    for s in ['12']:#sic_maximum_record_error_list:
        print(s, main_api+service_parameter+year+form_obj_sic+s+'&p_st=KY'+'&output='+output_type, dmr_external_dir)
        chunks_df = query_paginated(s, main_api+service_parameter+year+form_obj_sic+s+'&output='+output_type, state='KY', path=dmr_external_dir)
        dmr_df = pd.concat([dmr_df, chunks_df])
        print(s, main_api+service_parameter+year+form_obj_sic+s+'&p_st=WV'+'&output='+output_type, dmr_external_dir)
        chunks_df = query_paginated(s, main_api+service_parameter+year+form_obj_sic+s+'&output='+output_type, state='WV', path=dmr_external_dir)
        dmr_df = pd.concat([dmr_df, chunks_df])
    # Quit here if the resulting DataFrame is empty
    if len(dmr_df) == 0:
        print('No data found for this year.')
        exit()

    dmr_required_fields = pd.read_csv(data_dir + 'DMR_required_fields.txt', header=None)[0]
    dmr_df = dmr_df[dmr_required_fields]
    reliability_table = globals.reliability_table
    dmr_reliability_table = reliability_table[reliability_table['Source'] == 'DMR']
    dmr_reliability_table.drop(['Source', 'Code'], axis=1, inplace=True)
    dmr_df['ReliabilityScore'] = dmr_reliability_table['DQI Reliability Score']

    # Rename with standard column names
    dmr_df.rename(columns={'ExternalPermitNmbr': 'FacilityID'}, inplace=True)
    dmr_df.rename(columns={'Siccode': 'SIC'}, inplace=True)
    dmr_df.rename(columns={'NaicsCode': 'NAICS'}, inplace=True)
    dmr_df.rename(columns={'StateCode': 'State'}, inplace=True)
    dmr_df.rename(columns={'ParameterDesc': 'FlowName'}, inplace=True)
    dmr_df.rename(columns={'DQI Reliability Score': 'ReliabilityScore'}, inplace=True)
    dmr_df.rename(columns={'PollutantLoad': 'FlowAmount'}, inplace=True)
    dmr_df.rename(columns={'CountyName': 'County'}, inplace=True)
    dmr_df.rename(columns={'GeocodeLatitude': 'Latitude'}, inplace=True)
    dmr_df.rename(columns={'GeocodeLongitude': 'Longitude'}, inplace=True)
    dmr_df = dmr_df[dmr_df['FlowAmount'] != '--']
    # Already in kg/yr, so no conversion necessary

    #FlowAmount is not a number. Set FlowAmount to float64
    dmr_df['FlowAmount'] = pd.to_numeric(dmr_df['FlowAmount'], errors='coerce')

    # if output_format == 'facility':
    facility_columns = ['FacilityID', 'FacilityName', 'City', 'State', 'Zip', 'Latitude', 'Longitude',
                        'County', 'NAICS', 'SIC'] #'Address' not in DMR
    dmr_facility = dmr_df[facility_columns].drop_duplicates()
    dmr_facility.to_csv(set_dir(output_dir + 'facility/')+'DMR_' + report_year + '.csv', index=False)
    # # elif output_format == 'flow':
    flow_columns = ['FlowName']
    dmr_flow = dmr_df[flow_columns].drop_duplicates()
    dmr_flow['Compartment'] = 'water'
    dmr_flow['Unit'] = 'kg'
    dmr_flow.to_csv(output_dir + 'flow/DMR_' + report_year + '.csv', index=False)
    # elif output_format == 'flowbyfacility':
    fbf_columns = ['FlowName', 'FlowAmount', 'FacilityID', 'ReliabilityScore']
    dmr_fbf = dmr_df[fbf_columns].drop_duplicates()
    dmr_fbf['Compartment'] = 'water'
    dmr_fbf['Unit'] = 'kg'
    dmr_fbf.to_csv(set_dir(output_dir + 'flowbyfacility/')+'DMR_' + report_year + '.csv', index=False)

    file_name = data_source + '_' + report_year + '.csv'
    dmr_df.to_csv(path_or_buf=output_dir + file_name, index=False)


if __name__ == '__main__':
    main()



